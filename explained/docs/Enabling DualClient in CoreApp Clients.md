# Enabling DualClient in CoreApp Clients

*This document was started by [Lokesh Khurana](https://salesforce.quip.com/VJXAEAIuujB) to write down impact/work done for enabling DualClient as default Connection for phoenix-client*


## Motivation

Currently we are using **PhoenixConnection** as our default JDBC connection for our Phoenix Clients except VAGABOND which uses phoenix in HA mode and creates **ParallelPhoenixConnection** for one of their profile ([ACS_OPERATION_DR](https://sourcegraph.soma.salesforce.com/perforce.soma.salesforce.com/app/main/core@HEAD/-/blob/phoenix/java/src/phoenix/connection/PhoenixConnectionProfile.java?L36)). For [Consistent Failover for FKP migration](https://salesforce.quip.com/2emqADL4FHHl) we need a mechanism to failover to another cluster from current active cluster (in this case EKS to FKP cluster) and also for upcoming feature of consistent cluster failover. To make that possible we can use another type of connection which was build as part of phoenix HA :- **FailoverPhoenixConnection** , thus the motivation to enable ha mode in coreapp clients for all the profiles and making FAILOVER HA policy as default policy.


## Caveats

* The changes discussed are for connections created by core usecases to HBase Clusters.
* Need to think more for off-core/near-core usecases and if same can be applied for them.
* Only PhoenixConnections are considered this doesn’t include HConnection Traffic.
* For internal server side traffic related connections follow :- [PHOENIX-7493](https://issues.apache.org/jira/browse/PHOENIX-7493)

## Requirements

* All Core app usecases should use DualClient with FailoverPhoenixConnection (Except VagaBond)
    * It should not affect current working of Vagabond Client which will continue to use Parallel Policy
    * All the core app tests and FTests should succeed after making FailoverPhoenixConnection as default.
    * Every profile which is using FailoverPolicy should use same HAGroupName by default `(failover-<fi>-<fd>)` to make Failover faster.
* Once both the clusters are in standby state, make sure phoenix client do the right thing as described below
    * Client should not be able to create any new phoenix/hbase connections and throws the right exception.
    * Drain all the current active cluster connections
    * Drains all the read/write requests.
    * Throws the right exception `FailoverSQLException` and make sure all the application code is able to handle this exception.
* Once the failover is complete and new cluster has become active
    * All the new connections should go the new active cluster.
* Make sure ZKWatcher is correctly initialized and getting notified of all the changes happening to rolerecord
* HA Connections should be able to handle ZKLess connections to both Active and Standby clusters.
* *If HA Connection creation fails due to [phoenix.ha.group.name](http://phoenix.ha.group.name/) not present in property, should we through Exception or fallback to Single Cluster Connection (`phoenix.ha.fallback.enabled`) which is TRUE by default?*



## High Level Design (Work needs to be done) [PHASE-1]

### Data Mention

* Make `*PHOENIX_CONNECTION_PROFILE_DEFAULT_FAILOVER*` ConnectionProfile as default profile which will make ha enabled by default.
* Data Mention
    *  Check if any method which is exclusive to PhoenixConnection is being exposed and used by core.
    * There are many cases where Connections are unwrapped as PhoenixConnection or there are links present which is pointing to PhoenixConnection which also needs to be updated ([reference](https://sourcegraph.soma.salesforce.com/search?q=context:global+repo:%5Eperforce%5C.soma%5C.salesforce%5C.com/app/main/core%24%40HEAD+%28unwrap%28PhoenixConnection+OR+org.apache.phoenix.jdbc.PhoenixConnection%29&patternType=keyword&sm=0)).
    * These unwrapped PhoenixConnections are used to access APIs such as `getConnectionQueryServices()` or `getURL()`....
        * Make appropriate changes to expose the APIs which are meaningful and expose them at interface level (PhoenixMonitoredConnection) 
* Make sure we are setting `phoenix.ha.group.name` as `*failover-<FI>-<FD>*` ** for every profile except Vagabond and make it generic if any other profile wants to use Parallel Phoenix Policy should have their own group name.
    * HAGroupName for dev and autobuild environment should be set according to the CRR being created default being `failover-localhost-localhost`
* Make sure profile connections are able to override the `phoenix.ha.group.name` to set their own HAGroup if required.




### [✅] Local/Autobuild Testing Plan

* Data Mention :- Make sure dev and autobuild environment are able to create FailoverPhoenixConnection for testing.
    * Need to create CRR in local and autobuild environment to be able to get back FailoverPhoenixConnection from phoenix-cllient.
    * Need to add that step in `precheckin` and `precommit-producers (CRST)`
    * Cluster Role Record can be following for local testing
        *     {
                  "haGroupName" : "failover-localhost-localhost",
                  "policy" : "FAILOVER",
                  "zk1" : "localhost:2181:/hbase",
                  "role1" : "ACTIVE",
                  "zk2" : "fake-dummy-zk-server:2181:/hbase",
                  "role2" : "OFFLINE",
                  "version" : 1
                }
    * CRR for autobuilds can be following where we still need to figure out `${host.name}` value in every pipeline. For precheckin it is localhost but for CRST hbase runs in different VM than FTests.
        *     {
                  "haGroupName" : "failover-localhost-localhost",
                  "policy" : "FAILOVER",
                  "zk1" : "**${host.name}**:2181:/hbase",
                  "role1" : "ACTIVE",
                  "zk2" : "fake-dummy-zk-server:2181:/hbase",
                  "role2" : "OFFLINE",
                  "version" : 1
                }
    * We already have a step in ant [setupPhoenixHA](https://gitcore.soma.salesforce.com/core-2206/core-public/blob/960a3837178b1a92bb0a572ce963b9b21373ec3c/tools/Linux/hbase/build/build-core.xml#L461) need to test that properly and tweak it for above case.
* Test every phoenix related FTests
* PR :- https://gitcore.soma.salesforce.com/core-2206/core-public/pull/79891



### [WI] Dev Testing Plan

Get coreapp cell connected to our dev clusters for testing and test below scenarios

* Make Sure changes made to CRR are reflected to FailoverPhoenixConnection i.e. able to identify the state of urls and giving back right url to active clusters.
* After an ACTIVE cluster’s state changes to STANDY
    * Make sure client is not able to create any more phoenix connections to that cluster.
    * Make sure current connections drain
    * Make sure current read/write operations drains
    * Throws right exception `FailoverSQLException` and make sure coreapp is able to handle this exception.
* After we move from STANDBY to ACTIVE make sure all new connections are going to newly active cluster.
* When both clusters has different CRRs phoenix-client should be able to use latest versioned CRR out of two.
* Make sure PhoenixConnection under FailoverPhoenixConnection uses correct url irrespective of url provided for bootstrap, for example.
    * We have 2 cluster EKS-1a (ACTIVE) and EKS-1b (STANDBY)  with zk-1a and zk-1b urls for ZooKeeper quorums and hmaster-1a and hmaster-1b urls for HMaster quorum urls. So following scenarios should be true
        * [Image: ZK.png]
            * For this scenario FailoverPhoenixConnection should have PhoenixConnection with url as **zk-1a.**
        * [Image: hmaster.png]
            * For this scenario bootstrap call is using zk-1a and zk-1b in url from core app but FailoverPhoenixConnection should use **hmaster1a** url in underlying PhoenixConnection to create connection to cluster
* Make Sure ResultSet is being closed as part of connection close and clients see it when they do rs.next()
* If HA Connection creation fails we should throw appropriate Exceptions which can happen due to following reasons
    * If coreapp doesn’t set `phoenix.ha.group.name`
    * HA url (`[zk-1a | zk-1b]`) is wrong or one of the zk is not reachable and curator creation fails
    * CRR with the name of HAGroupName is not present.
    * Trying to create connection when Failover process has started.
* Run (corresponding to Phoenix) FTests from coreapp cell.
* Do same testing for NearCore as well.
* Run all Phoenix ITs with FailoverPhoenixConnection.
    * Run above ITs against Dual Cluster.
* Ask coreapp team to help us run one of their perf run and then test End To End Failover.
* Run Chaos Testing during End To End Failover 
    * For Both Active and Standby Clusters
* SfdcHbaseClient level introduce ITs for HAConnections
* Test the threadPool bug during failover
    * Do we need to manually depool EKS threadpools / Will there be restart ?




## Enable FailoverPhoenixConnection with Master/RPC Registry ([PHOENIX-7495](https://issues.apache.org/jira/browse/PHOENIX-7495)) [PHASE-2]

### Background

Timeline for DualClient to go in production is with 256 release and we are already enabling Master/RPC registry in 254. MasterRegistry requires HMaster quorum to be passed into url instead of ZKquorum and HMaster ports instead of ZK ports. So to enable MasterRegistry changes which are being done on core-app, needs to update url of single cluster PhoenixConnection from ZK to HMaster. 
               That means Bootstrap call to ZK doesn’t need to happen for Master Registry and when DualClient changes go in bootstrap url has to be ZK urls which overrides the work of MasterRegistry. So DualClient with support of Master/RPC Registry needs to go in 256 along with DualClient to prevent override of MasterRegistry support.

[PHOENIX-6523](https://issues.apache.org/jira/browse/PHOENIX-6523) provided support of different HBase Registry Implementations in phoenix through connection urls below are the examples

* jdbc:phoenix:hostname1,2,3...:<properties> (default -> ZK)
    jdbc:phoenix+zk:hostname1,2,3...:<properties> (ZK)
    jdbc:phoenix+hrpc:hostname1,2,3...:<properties> (RPC)
    jdbc:phoenix+bigtable:hostname1,2,3...:<properties> (Master)
* Based on the url ConnectionInfo types are [created](https://github.com/apache/phoenix/blob/33d060f629424e9df29988425e17077b5517cf71/phoenix-core-client/src/main/java/org/apache/phoenix/jdbc/ConnectionInfo.java#L161).

### High Level Design

* Connection url from clients for Phoenix HA Connections will remain same as it is now, i.e. using ZK quorum of the connecting clusters irrespective of the RegistryType. It will be for bootstrap call to get ClusterRoleRecords which contains actual info to use for underlying PheonixConnections.
* Introducing a new parameter in ClusterRoleRecord :- **RegistryType** to store the type of url CRR will be storing
    * Current ClusterRoleRecord contains 
        * HAGroupName :- HAGroupName/CRR Name
        * Policy :- Policy associated with the given CRR (Parallel/Failover)
        * ZK1 :- zk url to cluster 1 including quorum, port and additional JDBC params
        * Role1 :- Role Status of cluster 1 ( ACTIVE, STANDBY, OFFLINE, UNKNOWN)
        * ZK2 :- zk url to cluster 2 including quorum, port and additional JDBC params
        * Role2 :- Role Status of cluster 1 ( ACTIVE, STANDBY, OFFLINE, UNKNOWN)
        * Version 
    * New ClusterRoleRecord will look like this
        * HAGroupName :- HAGroupName/CRR Name
        * **RegistryType :- HBase registry type (ZK, Master, RPC)**
        * Policy :- Policy associated with the given CRR (Parallel/Failover)
        * **URL1** :- url to cluster 1 including quorum, port and additional JDBC params
        * Role1 :- Role Status of cluster 1 ( ACTIVE, STANDBY, OFFLINE, UNKNOWN)
        * **URL2** :- url to cluster 2 including quorum, port and additional JDBC params
        * Role2 :- Role Status of cluster 1 ( ACTIVE, STANDBY, OFFLINE, UNKNOWN)
        * Version 
    * If RegistryType is not present then for backward compatibility it will be assumed as ZK.
    * Updates will be required wherever we are assuming that url present in CRR is ZK url.
*  JDBC connection string is created based on url on roleRecord for underlying PhoenixConnection, which will be altered to one of [these](https://salesforce.quip.com/6ufDAKWZlhIR#temp:C:PPc63560ab8da0f4cc98743ae3c0) based on registryType, and thus it will create the ConnectionInfo to CQSI mapping automatically based on the registryType.














## Identified Bugs during testing

* ✅ We are creating one HAGroupName/CRR for every profile using FailoverPolicy.
    * We maintain a cache of <HAGroupInfo, HighAvailabilityGroup> at jvm level so that we don’t have to create HighAvailabilityGroup and get CRR for every connection.
    * HAGroupInfo contains 
        * url1, url2, name, additionalParams
        * additionalParams contains profile name (which is sfdc internal thing known as jdbcUrlIdentifier) used for profile based metrics.
    * Previously we had different name in HAGroupInfo foreach profile so giving back url was same as clients were using to create connection.
    * Now every profile uses same name and additional param is optional thing so for Key we use url1,url2,name and first profile creating connection with failover group name will be thrown back in url for every profile.
    * JIRA :- [PHOENIX-7502](https://issues.apache.org/jira/browse/PHOENIX-7502)
* In transitClusterRoleRecord method (which handle transition into new CRR from old based on policy) does nothing in case ParallelPhoenixConnection. 
* For ParallelPhoenixConnections we are using HAGroupInfo’s urls to create connections instead of roleRecords, which is fine for current state as both are ZK Urls of same pair of clusters but with MasterRegistry that won’t be true, so need to update ParallelPhoenixConnection to use roleRecord’s urls.
* There are checks at some places where we check if url we are going to use in PhoenixConnections under HAConnections are either one of HAGroupInfo’s url if not we throw exception
    * In `HighAvailabilityGroup.connectToOneCluster` there is a check of url with HAGroupInfo’s url which is wrong now as url we are getting is from CRR which can be HMaster url and HAGroupInfo contains url passed by client which will always be ZK.
    * The above 3 will be resolved as part of [PHOENIX-7495](https://issues.apache.org/jira/browse/PHOENIX-7495)
* ✅  For making tests work with HA Connection in autobuilds/locally we are creating clusterRoleRecord with dummy-zk-url for role2 but there are tests which create connections to DR cluster only by setting HBaseServiceDiscovererFactory.getInstance().getDRZookeeperQuorum() value to zookeeper.quorum. 
    * either need to remove those tests or reverse the quorums before test starts...
* For nearcore, we are storing single connection url and HAURL in same variable and then setting it to hbase.zookeeper.quorum, which created problems with threadpools evern after PHOENIX-7533.



## Work Items

|	|WI 	|	|Target Sprint	|Is required for FKP?	|Assignee	|Notes	|
|---	|---	|---	|---	|---	|---	|---	|
|1	|[W-18342127](https://gus.lightning.force.com/lightning/r/a07EE00002DHYe3YAH/view)	|[FKP Migration Testing] Basic testing for CRR using coreapp cell	|5b	|Y	|[Ritesh Garg](https://salesforce.quip.com/bNNAEAHxS7p)	|Resume the work since we have HA related client metrics	|
|2	|[_W-18341805_](https://gus.lightning.force.com/lightning/r/a07EE00002DHPweYAH/view)	|[FKP Migration Testing] Basic testing for FailoverPhoenixConnection using coreapp cell	|5b	|Y	|[Ritesh Garg](https://salesforce.quip.com/bNNAEAHxS7p)	|Resume the work since we have HA related client metrics	|
|3	|[_W-18342276_](https://gus.lightning.force.com/lightning/r/a07EE00002DHb9pYAD/view)	|[FKP Migration Testing] Figure out how to run one of the customer's perf runs and test End to End Failover during that scenario to understand the behavior	|	|Y(needed for prod)	|??	|Failover connection: HBPO is our customer
Parallel: Work with Vegacache.	|
|4	|[W-18342139](https://gus.lightning.force.com/lightning/r/a07EE00002DHl7BYAT/view)	|[FKP Migration Testing] Make Sure ResultSet is being closed as part of connection close and clients see it when they do rs.next()	|5b	|Y	|[Ritesh Garg](https://salesforce.quip.com/bNNAEAHxS7p)/[Lokesh Khurana](https://salesforce.quip.com/VJXAEAIuujB)	|Will be tested as a part of #1 and #2.	|
|5	|[_W-18342316_](https://gus.lightning.force.com/lightning/r/a07EE00002DHhCPYA1/view)	|[FKP Migration Testing] Introduce ITs for HAConnections at sfdc-hbase-client project	|6a	|Y(needed for prod)	|[Jing Yu](https://salesforce.quip.com/DDQAEAfaQFL)/[Lokesh Khurana](https://salesforce.quip.com/VJXAEAIuujB)	|Not needed as part of dev deployment but needed before prod rollout.	|
|6	|[_W-18342253_](https://gus.lightning.force.com/lightning/r/a07EE00002DHTZcYAP/view)	|[FKP Migration Testing] Run all PhoenixITs with FailoverPhoenixConnection	|5b	|Y(needed for prod)	|[Divneet Kaur](https://salesforce.quip.com/VOaAEAEH0hV)	|Create a new profile in phoenix IT.	|
|7	|[W-18342297](https://gus.lightning.force.com/lightning/r/a07EE00002DHl7YYAT/view)	|[FKP Migration Testing] Run Chaos Testing while running end to end Failover	|n/a	|N	|Need to find owner, most probably some one from phoenix team	|Not needed for FKP	|
|8	|[W-18342229](https://gus.lightning.force.com/lightning/r/a07EE00002DHwK1YAL/view)	|[FKP Migration Testing] Run FTests (corresponding to phoenix) through coreapp cell with HA Enabled for every profile	|	|Y	|[Rushabh Shah](https://salesforce.quip.com/YNFAEAfQqvI)	|Never'ming the work since we cannot run ftests from a live cell. See the discussion [here](https://salesforce-internal.slack.com/archives/C01Q3M3LUMB/p1747421743736699?thread_ts=1745527285.765449&cid=C01Q3M3LUMB).	|
|9	|[W-18342218](https://gus.lightning.force.com/lightning/r/a07EE00002DHZOkYAP/view)	|[FKP Migration Testing] When HAGroup object creation fails, make sure we are falling back to Single Cluster connection	|	|	|	|	|
|10	|[W-18394892](https://gus.lightning.force.com/a07EE00002Dd0zmYAB)	|Expose Active URL to which FailoverPhoenixConnections are connecting for Gridforce to use	|5b	|Y	|[Lokesh Khurana](https://salesforce.quip.com/VJXAEAIuujB)	|Working with [Saurabh Rai](https://salesforce.quip.com/LUMAEAJTz4d) on testing.	|
|11	|[W-18290030](https://gus.lightning.force.com/a07EE00002CsQD9YAN)	|Fix enabling phoenix HA in autobuilds	|5b/6a	|Y	|[Lokesh Khurana](https://salesforce.quip.com/VJXAEAIuujB)	|	|
|12	|[W-18360863](https://gus.lightning.force.com/a07EE00002DPZ5pYAH)	|Disable falling back to bootstrap URL if client is unable to connect to CRR urls.	|5b/6a	|Y	|[Lokesh Khurana](https://salesforce.quip.com/VJXAEAIuujB)	|This is a config change. Will be done after testing. [Lokesh Khurana](https://salesforce.quip.com/VJXAEAIuujB) ***we need to Ensure CRR is present for failover and create CRR for parallel in all clusters before enabling this.***	|
|13	|[W-18139173](https://gus.lightning.force.com/a07EE00002BmmZUYAZ)	|Enable phoenix.ha.enabled in core app for all profiles in core after successful cluster testing	|6a/6b	|Y	|[Lokesh Khurana](https://salesforce.quip.com/VJXAEAIuujB)	|End goal.	|
|	|	|Enable phoenix.ha.enabled in core app for autobuilds environment	|	|	|[Lokesh Khurana](https://salesforce.quip.com/VJXAEAIuujB)	|	|
|14	|[W-18423286](https://gus.lightning.force.com/a07EE00002Dn8cBYAR)	|FKP Migration - Thread Management in CoreApp Client	|5b	|Y	|[Ritesh Garg](https://salesforce.quip.com/bNNAEAHxS7p)	|Backport complete, coreapp dependency updated for 258. This feature is enabled by a config which is enabled for sdb403 cell. 
	|
|15	|[W-18535978](https://gus.lightning.force.com/a07EE00002EVXlTYAX)	|Profile to Threadpool allocation	|5b	|Y (needed for prod)	|[Sanjeet Malhotra](https://salesforce.quip.com/EUJAEA4FlCf)	|	|
|16	|[W-18055077](https://gus.lightning.force.com/a07EE00002BBVIVYA5)	|Failover E2E testing from EKS to FKP in dev - Non-Core	|	|Y	|[Ritesh Garg](https://salesforce.quip.com/bNNAEAHxS7p)/[Ashutosh Parekh](https://salesforce.quip.com/WBVAEAmP6ih)	|	|
|17	|[W-18066136](https://gus.lightning.force.com/a07EE00002BEr93YAD)	|Failover E2E testing from EKS to FKP in dev - Core App Client	|	|Y	|[Ritesh Garg](https://salesforce.quip.com/bNNAEAHxS7p)/[Ashutosh Parekh](https://salesforce.quip.com/WBVAEAmP6ih)	|	|
|18	|[W-18456480](https://gus.lightning.force.com/a07EE00002E0yT1YAJ)	|Adding tags to coreapp client	|5a	|Y	|[Ritesh Garg](https://salesforce.quip.com/bNNAEAHxS7p)	|	|
|19	|[W-18499813](https://gus.lightning.force.com/lightning/r/a07EE00002EIzsLYAT/view)	|Integration of new metric in failover script and updates based on new algorithm	|	|Y	|[Ashutosh Parekh](https://salesforce.quip.com/WBVAEAmP6ih)	|	|
|20	|[W-18301396](https://gus.lightning.force.com/a07EE00002Cy0rSYAR) + Execution in all FI/FDs	|Ensure CRR is present for failover and create CRR for parallel	|6b	|Y	|	|Execution also needed 	|
|21	|[W-18670307](https://gus.lightning.force.com/a07EE00002FKNFJYA5)	|Update message whether fallback is enabled	|	|N	|[Lokesh Khurana](https://salesforce.quip.com/VJXAEAIuujB)	|Using checkHBaseHAConnectionTest if we test with HA vagabond profile and parallel CRR is not present, we get message in log that fallback is enabled but indeed it its not. Logging needs to be fixed	|

### Releases

1. Backport all the changes required for dual client to 13.13? What 13.17 schedule looks like? 
2. All the client side changes are present in main. 



## Appendix 

* **Handling Service Discovery ([Ritesh Garg](https://salesforce.quip.com/bNNAEAHxS7p))**
    * For Phase-1, we will essentially be overriding the rpc client connection registry changes in 254 as the rpc client connection registry will only be applicable on single client. 
        * We will still rely on `hbase.zookeeper.quorum` and `hbase.dr.zoookeeper.quorum` to discover the hbase server and crr. We plan to move this to templated form and remove hardcoded strings in core app config
    * For Phase-2, the service discovery logic will still rely on `hbase.zookeeper.quorum` and `hbase.dr.zoookeeper.quorum` . These will be used during bootstrap to fetch/listen CRR and then use that CRR for creating connection. What this means is that property  `hbase.client.bootstrap.servers` should not be needed in core app xmls.



## References

* [Phoenix Client High Availability Framework](https://salesforce.quip.com/6THcAlQ8aDBc)
* [Design of Phoenix/HBase DR in Falcon](https://salesforce.quip.com/DWXzAwWla7bV)
* [Cluster Role AdminTool for Phoenix HA](https://salesforce.quip.com/ZJyiAdbaxHrz)
* [Managing Cluster Role Record for Phoenix HA](https://salesforce.quip.com/kEiFAZVtscPT)
* [Consistent Failover for FKP migration](https://salesforce.quip.com/2emqADL4FHHl)
* [Phoenix Service Discovery](https://salesforce.quip.com/Z7RsAU3obgYN)
* [Testing Phoenix HA with 1P/Local HBase Cluster](https://salesforce.quip.com/bqaWAqXVThYd)
* [Consistent Cluster Failover](https://docs.google.com/document/d/1pAlKsro-mD7nLey08oCJJhJS0Sf4LDxeX5mycZ5b4b0/edit?tab=t.0#heading=h.pfjicxriktfm)

