# Phoenix Stats Client Cache Design

# Overview

This document focus on the overall Stats Client Cache design.

Related Documents:

1. [_Phoenix Stats Work Tracking Dashboard_](https://salesforce.quip.com/90W8ApKbMIaw)
2. [_Phoenix Stats Non Blocking Cache design_](https://sfdc.co/phoniex-stats-nonblocking-cache)
3. [_Evaluation Of Prefix Encoding Used For Phoenix Stats_](https://docs.google.com/document/d/1NIS65g-CKY5HEmUkQZvCbHFzUykLb8KPHX-9i65XVwk)
4. _[Use Segment Tree to Organize Guide Post Info](https://salesforce.quip.com/taWiALFmhquO)_

## Distributed Cache VS. Client Cache

From architecture perspective, there are mainly two different approaches to implement Stats Cache — one is Distributed Cache; another is Client Cache w/o Query Server.

Compared to Client Cache, Distributed Cache has the following advantages:

1. There is almost unlimited capacity, so it has best scalability for increasing number of customers.
2. Avoid peak server load which happens in Client Cache approach — many clients access system stats table at the same time, and each results in a server side scan.

In the long term, Distributed Cache is a better approach. We can either use existing distributed cache systems, e.g. Memcached or Redis, or build our own distributed cache system based on consistent hash. The choice will depend on whether or  not the existing distributed cache systems provide all the features we want, e.g. non-blocking cache and reducing server side load, which are covered in this document.

In this document, we focus on Client Cache approach because of reasons listed below:

1. The current stats cache is Client Cache which is much more lightweight than Distributed Cache. Before we have a fundamental architectural change, we'd better discuss about and fully understand all the possible optimizations we can do with the current approach.
2. With current guidepost width (100 MB) and limited number of customers/scenarioes, it hasn't been proven that the current Client Cache approach has hit its ceiling. We need to collect more Stats Cache metrics to understand the Data Access Pattern before making fundamental change.
3. Both Distributed Cache and Client Cache approaches share some of the same problems. The optimizations that we mentioned in “Reduce Memory Footprint”, “Provide Flexible Eviction Policy”, “Provide TTL Setting Per Table”, “Reduce Sever Side Load”, “Improve Client Cache Data Integrity” and “Client Cache Cleanup” can also be fully or partially applied to Distributed Cache approach.

## Current

Below is the high level picture of the current Stats Client Cache. It is a non-blocking cache, and a cache instance is created for every Connection Query Service instance.
[Image: image.png]
### Non-blocking Client Cache

In one JVM, all Stats Client Cache instances share some background threads which periodically refresh the cached entries in the asynchronous manner. For details, please refer to the design document “[Use Asynchronous Refresh to Provide Non-blocking Phoenix Stats Cache](https://salesforce.quip.com/rxokAkVatiQO)”.

### Data Structure

The Stats Client Cache is built on top of Google Guava Cache which can be treated as a powerful ConncurrentHashMap with cache characteristics. The key of the cache is <table name, column family name>. Because the tables which use stats only has 1 column family, so now every entry in the stats client cache is the guide post info for a whole table.

# Client Cache High Level Design

Use the following table to track all the client side features.

|Optimization	|Status	|Priority	|
|---	|---	|---	|
|Use stats for Skip Scan 
[W-5415343](https://gus.lightning.force.com/lightning/r/a07B0000005ZH8yIAG/view)	|Design	|High	|
|Make Stats client cache Non blocking	|Done	|Pilot testing	|
|Reduce Memory Footprint - Reduce Granularity of Cache Entry	|Working on (ETA: 03/15)	|Pilot testing	|
|Reduce Memory Footprint - Achieve Better Compression	|Working on (ETA: 04/15)	|Pilot testing	|
|Reduce Memory Footprint - Support JVM Level Cache (Configurable)	|ETA:3/28/2019	|Pilot testing (High)	|
|Stats correctness for all types of queries(Skip scan, limits, RVC etc...)	|ETA:4/30/2019	|Platform Pilot (high)	|
|Perf testing Client cache improvements(above)	|ETA:3/28/2019	|Pilot testing	|
|Make Stats Cache Pluggable To Accommodate Distributed Cache	|ETA:4/30/2019	|Platform Pilot (high)	|
|Improve Stats Cache Data Integrity (Reduce Server side load)	|ETA:4/30/2019	|Platform Pilot	|
|Provide TTL Setting Per Table	|ETA:4/30/2019	|Platform Pilot	|
|Perf testing Client remaining cache improvements(above)	|ETA:5/15/2019	|Platform Pilot	|
|Provide Elastic Capacity and Flexible Eviction Policy
	|High Level Design Draft	|TBD	|
|
PHOENIX-XXXX Prefix encoding configurable	|High Level Design Draft	|TBD	|
|Move to PQS Thin Client and Cache on PQS servers	|High Level Design Draft	|TBD(depends on public cloud + PQS for Core)	|

## Reduce Memory Footprint

### Support JVM Level Cache (Configurable)

The following picture is the high level block diagram of Current Stats Client Cache. In one JVM, one stats client cache instance is created for every Connection Query Service instance. In core app, for now, there are 5 different connection profiles — “Default”,  “Async Operation”, “Real Time Operation”, “Pliny Real Time Operation” and “Update Statistics Operation”. Because different settings for different connection profile, including “**phoenix.query.timoutMs**”, “**hbase.client.pause**” and “**hbase.client.retries.number**”, aren't applied to Stats Cache, there is huge opportunity to provide JVM level stats client cache to reduce duplicates among all stats client cache instances.
[Image: Stats Cache (Connection Query Service Level).jpg]The following picture is the high level block diagram of JVM level Stats Client Cache. For each of query connection profile, we can specify whether it is at JVM level or query connection service level. 
[Image: Stats Cache (JVM Level).jpg]
### Reduce Granularity of Cache Entry

Eventually, we need to be able to just cache a part of guide posts of a table, e.g. for a tenant  specific view or a special query. This problem is tracked by [PHOENIX-4927](https://issues.apache.org/jira/browse/PHOENIX-4927) “Disentangle the granularity of guidepost data from that of client cached guide post data”.

To support caching stats per tenant specific view, the key of status client cache will be changed to the tuple <table name, column family name, key range>.

To support caching stats per query, we can build an index on top of stats client cache. The key of status client cache will be changed to the tuple <table name, column family name, key range>, and this is also the key for index implement by treeMap (using B* Tree provided in standard libraries). If a new entry isn't overlapped with the existing entries, it will be simply as checking and inserting the entry into both index and client cache; otherwise we can use cellingEntry/lowerEntry/higherEntry/lowerEntry APIs provided by treeMap to find the entries which have overlap with the new entry, then merge the stats of these entries into the new entry, finally update both index and the stats client cache.

### Achieve Better Compression

Using prefix encoding might have NOT achieved the original goals set in PHOENIX-2417 - reduce footprint in the memory and over-the-wire. According to my document "[Evaluation Of Prefix Encoding Used For Phoenix Stats](https://docs.google.com/document/d/1NIS65g-CKY5HEmUkQZvCbHFzUykLb8KPHX-9i65XVwk)" (Case 3: Real Data From Platform Team), the cached stats (GPW - 100MB) reduce only about 10% with prefix encoding based on very rough calculation.

The Variant Segment Tree described [in the document](https://salesforce.quip.com/taWiALFmhquO) is designed to support different encoding scheme or compression algorithms. The main idea is that the guide post chunk in leaf node is the unit of encoding/decoding or compression/decompression, i.e., it is always encoded/decoded or compressed/decompressed as a whole. We can choose better compress algorithm to compress the whole lead node which contains not only guide posts in key representation but also estimation info (# rows, # bytes, the update time stamp) for each guide post.
[Image: The Variant Segment Tree Overview.jpg]
A related Work Item W-5561235 "PHOENIX-XXXX Prefix encoding should be configurable at table level" was opened to track the issue.

## Provide Elastic Capacity and Flexible Eviction Policy

As the number of tables and the size of each table continuous grow, it still could be out of memory within a single JVM or “Client-side evictions can occur when the cache is filled by other use cases". There are several ways to handle this problem.

1. Increase JVM memory allocation for Core App.
2. Set size limit on the stats of each table. After Stats Loader loads the stats from the System Stats table, if its size plus the total size of the cached entries of the same table exceeds the size limit, it throws the exception which is caught by Stats Cache and then handled by step 3 and step 4.
3. Stats Cache can provide some kind of “dynamic sizing” feature —  JVM still has enough spare memory, we can create another instance of Guava Cache instance and release it when memory is under pressure based on eviction policy described at step 4.
4. Provide LRU or priority-based LRU eviction policy. For the latter, we define different priorities for different tables using stats. Stats Cache always removes the stats of the tables with lowest priority first; for the stats of tables having the same priority, use LRU.
5. If we eventually use Phoenix Query Server for Core, we can Horizontally partition query servers on tables, i.e.  each query server only handles queries on a subset of tables and only caches stats for those tables. By doing so, different layers on PQS can also have benefits because of better data locality.

## Provide TTL Setting Per Table

Now the duration passed to CacheBuilder.refreshAfterWrite() is the duration to check whether we need to reload a particular cache entry, which is denoted as T_Check. The refresh duration of a table is defined to be the integral multiple of T_Check. The StatsLoader.reload() is invoked for every period of T_Check in which needsRefresh() will be called to check whether we need to reload the stats for the given cache entry based on the refresh duration of the table.

## Reduce Sever Side Load

Currently, for each cached entry, every refresh results in a server side scan which increases the server side load. Because of this reason, the refresh period needs to be long enough and 

To remove the above pain point, one of approaches is:

1. Create System Stats Meta Table which has five columns “table name”, “column family name”, “start key”, “end key” and “the last update time”. The first columns comprises the primary key. A new row is inserted or an existing row is updated, after an entire “UPDATE STATISTICS” SQL Command or a MR job succeeds. 
2. The stats loader keeps the above “the last update time” seen in every refresh and use it the for next refresh. The stats load actually query System Stats table only after the entry in the System Stats Meta Table is changed.

To avoid peak server load which happens when multiple clients access system stats table at the same time, each stats client side needs to wait a random time (< T_Check) for its first time refresh. 

With the above optimization, we can have more frequent refreshes but much less frequent server side scans. 

## Improve Stats Cache Data Integrity

With the approach in “Reduce Sever Side Load” section, we can also improve stats cache data integrity.

## Make Stats Cache Pluggable To Accommodate Distributed Cache

Stats Cache needs to be pluggable so that it can be switched between client-based cache and distributed-based cache.

## Move to PQS Thin Client and Cache on PQS servers

Moving to the PQS thin client will reduce the memory pressure of running the core app as well as the phoenix client on the same machine.  All of the stats caching will occur on the PQS side as opposed to the PQS client.

## Risks

1. Clarity on usage of stats since platform is exposing it to customers
2. Platform Data grows faster and larger than expected and our cache is in JVM and the cache size is proportional to the table size there could be issues for very large tables.

## What we need from Platform team

1. What are all types of queries we are going to let platform customers use
    1. Like Limits, RVC, range scan?
2. Table size estimates
    1. 100TB 



