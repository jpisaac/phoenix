# Phoenix Client High Availability Framework

*Last modified: Friday, October 23, 2020*

## 1. Introduction

To improve availability, failing over between two HBase clusters is a new project in Falcon, see [Design of Phoenix/HBase DR in Falcon](https://salesforce.quip.com/DWXzAwWla7bV). Meanwhile, exploiting two HBase clusters is considered a solution to Vagabond use case in 1P, see [Multi-Cluster Phoenix Client Design](https://salesforce.quip.com/VyKUAaZa9yjG). The two project intrinsically have some similarities: exposing two HBase clusters to SFDC clients, as discussed in Eric’s doc: [Replication in Phoenix](https://salesforce.quip.com/lapKAp7bWpUT). To share the design and development effort, this doc proposes a high availability framework (called “the framework”) in Phoenix client. The two use cases, DR in Falcon and Vagabond in 1P, will then be “plugged” into this high availability (HA) framework as concrete HA policies.

The framework enables high availability at Phoenix layer by allowing a Phoenix application to talk to two HBase clusters, sequentially or in parallel, so that one cluster being unavailable does not necessarily result in HBase service unavailability. There are three key concepts in this HA framework:

* **HA group**. An HA group is an association between a pair of HBase clusters, a group of Phoenix clients, and a HA policy (see below). This association from HA group to clients is a 1:N relation, that is, a given pair of HBase cluster serve a group of clients, but a Phoenix connection is confined to this HBase cluster pair for its lifecycle. The pair of HBase clusters should have enabled “master-master” cross-cluster replication, be it *asynchronous* or *synchronous -* **** though we at Salesforce have been supporting asynchronous replication. The HA groups are pre-defined such that a client needs to specify the group name when connecting to the pair of HBase clusters in that HA group. Having the same pair of HBase clusters in multiple HA groups allows clients to be grouped based on different availability and consistency requirements, along with coarse-grain load balancing.
* **Cluster role record**. In an HA group, each HBase cluster has a cluster role (ACTIVE or STANDBY, OFFLINE). The cluster role is stored in a metadata store (e.g. S3/Vault) and cached in the ZK clusters serving HBase. The cluster roles are initially maintained by human operator or an external system, and are honored by all clients in this HA group. The cluster will be interpreted with the aid of the HA policy. When the policy dictates that only one cluster should be used at a time, i.e., the conventional failover policy is used, if an HBase cluster has the STANDBY role in an HA group, all clients in that HA group will get fenced for both read and write operations. Usually in an HA group, one HBase cluster is ACTIVE and the other is STANDBY, meaning all requests in this group should go to the ACTIVE cluster. If the cluster role is changing from ACTIVE to STANDBY, “failover” will happen at all clients in this group when they are notified this failover event. The framework will stop all in-progress operations on this cluster. Connections will connect to the other HBase cluster according to the HA policy. This provides best-effort consistency guarantee among all clients in this group. It is possible that the HA policy allows both clusters to be used, meaning there is relaxed consistency among clients since they can connect to either of the HBase cluster and try the other in case of failure. The cluster of ACTIVE role will be favored by the framework (not client) because in this HA group the ACTIVE cluster is assumed either “closer” (in the same datacenter in 1P) or “healthier”. This will be explained further later.
* **HA policy**. Every HA group has an associated HA policy which specifies how to use the HBase clusters pair. For `FAILOVER` HA policy, the connection will be established against the ACTIVE cluster always. In future, we can make this policy talk to the ACTIVE cluster and falls back to the STANDBY cluster in case of failure. But for any point of time, only one cluster is connected by the JDBC connection. For `PARALLEL` HA policy, the same operation can be sent to two clusters in parallel, meaning the client keeps the first connection while sending requests to the other cluster. Pure parallel policy will send client request simultaneously; hedge policy will send the request to ACTIVE cluster first and some time later (e.g. 5ms) if no response, the same request will be sent to STANDBY cluster. Those are the basic two categories of the HA policy, while more policies can be extended and plugged into the framework.

The overall architecture is as following. As dictated by the figure, a JDBC string with special format `jdbc:phoenix:zk1,zk2,zk3|zk1',zk2',zk3'` will enable the HA feature when creating a Phoenix connection. The key information includes two HBase cluster’s endpoint (ZK `address:port:/hbase`) whose order does not matter. HA group name is specified in the connection properties and will be used to retrieve cluster role and HA policy information of that HA group. When connecting, the client will get a JDBC Connection implementation, which can be failover Phoenix connection or parallel connection. Using this JDBC connection for creating `Statement` or querying a `ResultSet` does not require any application code change. Internally, the implementation will serve incoming client operation requests accordingly.

During creation, the HA group will get initial cluster role information and HA policy from the cluster role record. It caches the data and starts a watcher for future znode changes. When cluster role changes, the HA policy will take actions in its implementation, be it `FAILOVER` or `PARALLEL`, so that all threads in the JVM will get the context about new cluster role. We uses ZK here because it is built-in Hadoop ecosystem deployments as metadata synchronization, and is mature enough for HA framework to create dependency on. The ZK access is not on client read/write path because the lightweight watcher is managed by the HA framework only for responding cluster role record changes (infrequent event). Replacing ZK with any other meta-data service seems possible and is totally transparent to applications, though a pulling logic to get latest data might be required if it is not event-based.
*Figure: every HA group will have its own CQSI instance, and hence HBase connection. For common cases without cluster role change, the client’s connect request will be served fairly fast and simple: get, wrap a Phoenix connection and return. Clients watch the cluster role records stored in two ZK nodes so that it will get notified if there is any change. Upon those changes, the HA framework in client side will take actions - failing over connections to newly ACTIVE cluster, and/or update “favored” cluster information according to its HA policy.  During this process, other HA groups and non-HA connection will not be affected. This is based on the design that, CQSI is not shared by HA and non-HA connections, or by multiple HA groups.* 

## 2. High Availability (HA) Use Cases

All use cases will still use the JDBC abstraction to talk to Phoenix, and there is no code change other than the JDBC connection string and HA group name property. Consider the following code sample in *existing* application code:

```
String jdbcString = "jdbc:phoenix:zk1,zk2,zk3";
try (Connection conn = Driver.getConnection(jdbcString, props)) {
    .... // Use conn for some operations
    conn.createStatement().execute("UPSERT INTO table1 VALUES ...");
    conn.commit();
}
```

The new default way of using a pair of HBase cluster with HA enabled, would be:

```
String jdbcString = "jdbc:phoenix:[zk1,zk2,zk3**|zk4,zk5,zk6]**";
**props.set("phoenix.ha.group.name", "vagabond-p1");**
try (Connection conn = Driver.getConnection(jdbcString, props)) {
    .... // Use conn for some operations
    conn.createStatement().execute("UPSERT INTO table1 VALUES ...");
    conn.commit();
}
```

Behind the screen, there are several implementations to support those behaviors with different consistency and availability requirement. The two HA policy, `FAILOVER` and `PARALLEL`, are the two main categories. The former will make sure at a point of time, a client is confined to one cluster (the one with ACTIVE cluster role), the latter will send client’s requests to two cluster in parallel, and consolidate the results before returning to users (e.g. the faster wins, merge results).

### 2.1 Failover explicitly

With this policy, the HA framework fails over to the other cluster in a brutal way: it closes all existing Phoenix connections in this HA group as well as the associated active CQSI instance. Clients trying to use those closed Phoenix connections will get an `SQLFailoverException` exception, and they have to take actions in ***explicitly***. The action can be two kinds: retry the whole business logic which will get a new Phoenix connection to the new ACTIVE cluster, or call `failover()` method if it needs to reuse the Phoenix connection object. Application can choose to add some cleanup logic in-between the retries of business logic. 

This is designed for DR in Falcon project. This failover policy assumes that HA framework does not understand or is able to transfer the client context from one Phoenix connection to another connection to the other HBase cluster. With this policy, the HA framework closes the existing connections and hint the client with the reason why the connection gets closed, while the clients will need to acknowledge failover, check the applications layer consistency, and then take actions (retry or call `failover()`) accordingly. Clients can take precautions against potential data inconsistencies at application’s level. Consider the consistency problem is cooperatively solved between the application and the failover framework which honors cluster ACTIVE/STANDBY role. The failover framework knows cluster role transitioned, while the application knows what state should be rewritten or invalidated just in case some in flight stuff won't be replicated until much later.

A sample code snippet, which retries the business logic for up to 3 times is as following:

```
public void updateGusFeed(String feed) throws SQLException {
    int retry = 0;
    while (retry < 3) {
        try{
            updateGusFeedHelper(feed);
        } catch (SQLFailoverException e) {
            LOG.warn("Got failover exception, retry for the {} time", retry, e);
            retry++;
        }  // All other Exception will make business logic fail immediately
    }
}
private void updateGusFeedHelper(String feed) {
    String jdbcString = "jdbc:phoenix:fg1[zk1,zk2,zk3|zk4,zk5,zk6]";
    props.set("phoenix.ha.group.name", "ha1");
    try (Connection conn = Driver.getConnection(jdbcString, props)) {
        conn.createStatement().execute("UPSERT INTO table1 VALUES ...", feed);
        conn.commit();
    }
}
```

To assist CoreApp, we will update the connection provider implementation so that the `jdbcString` will be using two HBase cluster’s ZK. Meanwhile, the HA group name will be provided by customer when getting connection in a similar way to specifying the connection profile. Alternatively we can infer the HA group from connection profile for starters, so client will not need to change any code. The retry logic might already be in existing code in CoreApp in case of `SQLException`, or they can add that one using above approach catching the `SQLFailoverException` which is a subclass of `SQLException`.

This will be the default pattern when we enable DR in Falcon feature. More details can be found in the design doc: [Design of Phoenix/HBase DR in Falcon](https://salesforce.quip.com/DWXzAwWla7bV).

### 2.2 Parallel operations on two clusters

With this policy client when getting a JDBC connection will get two PhoenixConnections, each of which connect to one HBase clusters in the HA group. The implementation of this `PARALLEL` HA policy can vary according to client requirements. To start a request, it can send the request to the two clusters in parallel; or alternatively, it can send the request to the ACTIVE cluster, and some time later (e.g. 5/10ms) to the STANDBY cluster without cancelling the first one. To return a response, it can decide how to consolidate the results of two Phoenix connections to clients: it can return the fast response and ignore the slower one; or alternatively, it can wait for the other connection if the first response is empty for a query. 

In case of cluster role change, there is no “failover” action to be taken. Instead, the `PARALLEL` HA policy will simply set the new ACTIVE cluster as the favored cluster, and stop trying to use the `OFFLINE` cluster if any. To support this, there will be a different shim layer from failover at Connection/Statement/ResultSet level which wraps states of multiple threads connecting to multiple clusters. Code snippet, which internally talks two cluster in parallel (without failing one first), is like:

```
String jdbcString = "jdbc:phoenix:[zk1,zk2,zk3|zk4,zk5,zk6]";
props.set("phoenix.ha.group.name", "parallel2");
try (Connection conn = Driver.getConnection(jdbcString, props)) {
    // mutate could be on cluster 1 (zk1,zk2,zk3)
    conn.createStatement().execute("UPSERT INTO t1 VALUES ...");
    conn.commit();
    // query (Get) could be on clsuter 2 (zk4,zk5,zk6)
    ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM t1 where id=1");
    ... // visiting rs iterator
}
```

If you look closely, you would find this is no different from the original code other than using the two clusters in the JDBC string and set the HA group name in properties. The HA policy for this group is `PARALLEL`, but it is not specified by the client because that is in the HA group definition. The information about HA policy is in the cluster role record. The detailed design for the parallel behavior, including threading model, error handling, logging and metrics, are all covered in [Multi-Cluster Phoenix Client Design](https://salesforce.quip.com/VyKUAaZa9yjG).

### 2.3 Failover automatically (client side)

The failover explicitly policy does not provide much automatic feature because the application needs to get notified and to take action in case of exceptions at application level. Some immutable use cases do not need strong consistency during failover - this is assumed most of the use case. It is a more blessed feature to applications if it requires little to no interruption to clients. However, if we failover from one cluster to another automatically without telling clients, the first problem is about uncommitted mutations. They will be lost if the failover HA policy creates and wraps a new “blank” phoenix connection to replace the closed connection. There might be some ways of “copy-and-paste” mutations between two Phoenix connections, but that would be potentially major change. After addressing that, if there are multiple commits for a business logic, chances are those commits are committed partially into two HBase clusters. This could happen when multiple commits are called, or when auto-commit is enabled and multiple executions are issued. It seems not a problem if application is fine with the eventual consistency model, since those data will be replicated between the two HBase clusters asynchronously and eventually.

One possible improvement to availability is connection-level failover, without global coordination. The HA policy will treat the two clusters as a mirror system where both are ACTIVE. With this policy, a connection will automatically failover to the other cluster in multiple use cases: *1)* current HBase cluster is no longer ACTIVE, *2)* the ACTIVE cluster has server-side issues and throws exceptions like `DoNotRetryException`, and *3)* client can not connect to ACTIVE cluster in case of network issues etc. This can support multiple use cases in future. Specially, this can be used by Vagabond use cases if they prefer the “**dual cluster, sequential write**” model, see design doc for this in [VBase Architecture](https://salesforce.quip.com/xb8OAwGCExXY#bXUACAApgjv).

Code snippet, which automatically fails over to the other cluster if global failover happens or the ACTIVE cluster master is not running, is like:

```
String jdbcString = "jdbc:phoenix:[zk1,zk2,zk3|zk4,zk5,zk6]";
props.set("phoenix.ha.group.name", "ha3");
props.set("phoenix.ha.failover.policy", "FAILOVER|UNHEALTHY");  // | is OR
try (Connection conn = Driver.getConnection(jdbcString, props)) {
    conn.createStatement().execute("UPSERT INTO table1 VALUES ...");
    conn.commit();
    // it may failover to another cluster automatically
    ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM table1");
    ... // visiting rs iterator
}
```

## 3. Design and Implementation

### 3.1 Framework: HA group and policy

The Phoenix HA framework will provide the common code for both explicit `FAILOVER` (Falcon) and `PARALLEL` (1P), and it also opens the opportunity of supporting automatic client side failover. The HA group and their HA policy names are defined by human in cluster role records, which are stored and maintained in a central place, and populated into the ZK nodes for client watchers. The HA group class in HA framework would provides following functionalities:

* ***Get***. This will get an HA group instance from cache in the JVM given the connection information including HA group name and ZK cluster endpoints. This will internally create and initiate the CQSI instance accordingly. Specially, for `FAILOVER`, it only requires to open CQSI for ACTIVE cluster while for `PARALLEL` it will require two CQSI instances.
* ***Create ZK watcher***. When a new HA group is initialized, it should start the ZK watcher for the cluster role records stored in both ZK cluster nodes. This will also register HA policy-specific callback functions to handle the cluster role change on ZK side.
* ***Create ACTIVE connection***. It will create a Phoenix connection against the ACTIVE cluster with current context. This is mainly used by the `FAILOVER` HA policy since it will create and wrap a connection to ACTIVE cluster.
* ***Create cluster-specific connection***. It will provide a Phoenix connection to either cluster. This is mainly used b `PARALLEL` HA policy since it will need to create and wrap two connections at the same time.

The HA policy implementation will different in two ways: *1)* provide a JDBC to client, and *2)* deal with cluster role transition. So the interface will be like:

```
enum HighAvailabilityPolicy {
    /**** Provides a JDBC connection from given connection string and properties.
     *
     * @param haGroup The high availability (HA) group
     * @param info Connection properties
     * @return a JDBC connection
     * @throws SQLException if fails to provide a connection
     **/*
     abstract Connection provide(HighAvailabilityGroup haGroup,
                                 Properties info)
            throws SQLException;

 */*** Call-back function when a cluster role transition is detected in the HA group.
     *
     * @param haGroup The high availability (HA) group
     * @param oldRecord The older cluster role record cached in this client
     * @param newRecord New cluster role record read from one ZooKeeper cluster znode
     * @throws SQLException if fails to handle the cluster role transition
     */
    abstract void transitClusterRole(HighAvailabilityGroup haGroup,
                                     ClusterRoleRecord oldRecord,
                                     ClusterRoleRecord newRecord)
            throws SQLException;
}
```

### 3.2 Operation: cluster role record and tool

Once deployed, the only operation we need to do is to update the cluster roles upon which clients can get notified and start failing over process automatically. The cluster role record will include following information. Specially, the HA policy for this group is defined in the cluster role record, instead of asking clients to provide that. The reason is that, the HA policy should be honored by all clients, and it may violate the availability/consistency requirement for the HA group if it allows client to set different policies. The `version` filed is important because client will register ZK watchers to two clusters, and data on the two ZK cluster can be different since we have no way of updating two ZK clusters atomically. Client will trust any cluster role data it reads from either ZK cluster. If it reads two different ones, it will use the one with higher version.

```
/**
 * Immutable class of a record in cluster role store for a pair of HBase clusters.
 */
class ClusterRoleRecord {
    private final String haGroupName;
    private final HighAvailabilityPolicy policy;
    private final String zk1;
    private final ClusterRole role1;
    private final String zk2;
    private final ClusterRole role2;
    private final long version;
    // ... getter (no setter) and other supporting methods
}
```

There are multiple ways of maintaining the cluster role records in consistent and highly available ways. Discussion about the current and initial design can be found in doc [Cluster Role AdminTool for Phoenix HA](https://salesforce.quip.com/ZJyiAdbaxHrz).

### 3.3 How many HA groups do we have?

**In 1P**, according to design doc [HA Group possible solutions and processes for pod migration and site switches in 1p](https://salesforce.quip.com/tSORAZBcVZuJ), for each SuperPod HBase cluster and each Phoenix connection profile, we should create one HA group. Its HA group name should be Phoenix connection profile name, data center and HBase cluster name. As of Jan 2021, the only supported HA group in 1P is for Vagabond use case. Examples are:
`acsopdrHA-hbase3c-phx`: by default PHX cluster is ACTIVE and DFW cluster is STANDBY
`acsopdrHA-hbase3c-dfw`: by default PHX cluster is STANDBY and DFW cluster is ACTIVE

**In Falcon,** we create one HA group for one Phoenix HA connection profile. However, there will not be any cluster name encoded into the HA group because in Falcon there is no DR for CoreApp. It is the same set of CoreApp hosts that will access to the two HBase clusters, and the latency from CoreApp to two HBase clusters is the same. To use similar format to 1P, the HA group name in Falcon is composed of Phoenix connection profile name, FI and FD information. Example is:
`drinfalcon-dev1-uswest2-core002`: by default hbase1a cluster is ACTIVE and hbase1b cluster is STANDBY

### References

* [Replication in Phoenix](https://salesforce.quip.com/lapKAp7bWpUT) - the best doc for Phoenix replication at Salesforce *for §1 of this doc*
* [Design of Phoenix/HBase DR in Falcon](https://salesforce.quip.com/DWXzAwWla7bV) - DR in Falcon doc with `FAILOVER` policy *for §2.1 of this doc*
* [Multi-Cluster Phoenix Client Design](https://salesforce.quip.com/VyKUAaZa9yjG) - to support Vagabond use case with `PARALLEL` policy *for §2.2 of this doc*
* [Cluster Role AdminTool for Phoenix HA](https://salesforce.quip.com/ZJyiAdbaxHrz) - operation made simple *for §3.2 of this doc*

~*fin~*
