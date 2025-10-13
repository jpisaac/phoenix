**Vagabond NG**  
*By Azeem Mohammad, May 2018*

[**Introduction**](#introduction)	**[2](#introduction)**

[**Architecture**](#architecture)	**[2](#architecture)**

[Current Architecture](#current-architecture)	[2](#current-architecture)

[Limitations of current architecture](#limitations-of-current-architecture)	[2](#limitations-of-current-architecture)

[Vagabond-NG Architecture](#vagabond-ng-architecture)	[3](#vagabond-ng-architecture)

[Approach](#approach)	[4](#approach)

[Performance considerations](#performance-considerations)	[4](#performance-considerations)

[Availability Considerations](#availability-considerations)	[4](#availability-considerations)

[Option 1: Ignore the HBase write failures](#option-1:-ignore-the-hbase-write-failures)	[4](#option-1:-ignore-the-hbase-write-failures)

[Option 2: Handle the HBase write failures](#option-2:-handle-the-hbase-write-failures)	[5](#option-2:-handle-the-hbase-write-failures)

[Scalability](#scalability)	[5](#scalability)

[Administration & Monitoring](#administration-&-monitoring)	[5](#administration-&-monitoring)

[Comparison of new architecture with current architecture](#comparison-of-new-architecture-with-current-architecture)	[5](#comparison-of-new-architecture-with-current-architecture)

[**Storage Design**](#storage-design)	**[6](#storage-design)**

[HBase Table design](#hbase-table-design)	[6](#hbase-table-design)

[Schema](#schema)	[6](#schema)

[HBase Capacity](#hbase-capacity)	[7](#hbase-capacity)

[HBase TTL](#hbase-ttl)	[7](#hbase-ttl)

[CaaS](#caas)	[7](#caas)

[Data Types](#data-types)	[7](#data-types)

[CaaS Capacity](#caas-capacity)	[7](#caas-capacity)

[CaaS TTL](#caas-ttl)	[8](#caas-ttl)

[Migrating to new storage(CaaS+HBase)](#migrating-to-new-storage\(caas+hbase\))	[8](#migrating-to-new-storage\(caas+hbase\))

[**Step1 : Vagabond → Vagabond-Dual ( Primary: Vagabond)**](#step1-:-vagabond-→-vagabond-dual-\(-primary:-vagabond\))	**[9](#step1-:-vagabond-→-vagabond-dual-\(-primary:-vagabond\))**

[**Client Types**](#client-types)	**[9](#client-types)**

[Cursor use-cases](#cursor-use-cases)	[10](#cursor-use-cases)

[Blob use-cases](#blob-use-cases)	[10](#blob-use-cases)

[Miscellaneous use-cases](#miscellaneous-use-cases)	[11](#miscellaneous-use-cases)

[**Related Documents**](#related-documents)	**[11](#related-documents)**

# **Introduction** {#introduction}

Vagabond is an object store which is primarily used for 2 use cases : storing query results (aka cursor service) of SOQL and for storing semi-persistent data such as apex debug logs etc. Vagabond is currently deployed on a 4 node cluster as a pod level service and internally uses berkeley db (for metadata and small objects) and local file system. A custom clustering and replication logic is used for high availability and to minimize data loss.

This document covers the re-architecture of vagabond to be a spod-level/kingdom level solution which is horizontally scalable using standard storage services in our infrastructure.

# **Architecture** {#architecture}

## **Current Architecture** {#current-architecture}

![Vagabond-logical-diagram.png][image1]

### **Limitations of current architecture** {#limitations-of-current-architecture}

1. Not horizontally scalable  
2. Low utilization due to POD level service & fixed 4 node cluster  
3. No option for SPOD/Kingdom level use cases  
4. Uses custom storage implementation  
5. Vagabond’s data will be lost during site switch.

	[https://salesforce.quip.com/bdRAASCjmOwK](https://salesforce.quip.com/bdRAASCjmOwK)

6. Need to worry about disk failures and follow-up with infrastructure.

## **Vagabond-NG Architecture** {#vagabond-ng-architecture}

To overcome the limitations of current architecture, it is ideal if we can find a standard storage service which is suitable for the vagabond use cases. HBase is closest match for replacing the current vagabond server functionality(We considered BlobStore as well \- BlobStore is not ready yet as well as they are no throughput and latency guarantees). By using a combination of CaaS and HBase we intend to boost performance while also improving the availability during hbase downtimes.   
As per the current usage, there is a less time difference between PUT and GET for a key (80% of GET requests are within 15 minutes from PUT request , 90% of GET requests are within 60 minutes from PUT request).  So most of the GET requests can be served from CaaS by keeping the PUT request data in CaaS with TTL as 60 minutes.

### **Approach** {#approach}

1. On PUT request, new client writes the data to both CaaS and HBase.  
2. On GET request, new client first tries to fetch data from CaaS, if it is not available then it gets it from HBase.   
3. Failed HBase write operations are queued in Ajna/CaaS to playback later in background job  
4. Background job runs on multi-node

### **Performance considerations** {#performance-considerations}

PUT incurs a write to HBase and CaaS.  Hence the performance of writes will be limited by the performance characteristics of HBase. Writing to HBase can be done asynchronously using thread pool with limited pool size and executing writes in caller thread when pool is busy. 

GET performance can be improved by adjusting CaaS capacity. The more the CaaS capacity, the more data can be served from the CaaS, which improves the latencies.

Vagabond p90 latency for GET,PUT operation is \< [5ms](https://splunk-web.crz.salesforce.com/en-US/app/publicSharing/PublicOtherDash_vagabond_usage_metrics_view?form.timeRange.earliest=-60m%40m&form.timeRange.latest=now&form.instance=*&form.clientType=*&form.putLatencySpan=1m&form.getLatencySpan=1m&form.minutesWindow1=5&form.minutesWindow2=5&form.apiRadio=I&form.apexBatchRadio=I&form.syncStateRadio=I&form.apexCSIRadio=E&form.apexDebugRadio=E&form.reportResultsRadio=E&form.platformMonitoringRadio=E&form.deployRadio=E&earliest=0&latest=)  
HBase p90 latency for GET,PUT operation is \< [10ms](https://splunk-web.crz.salesforce.com/en-US/app/publicSharing/hbase-perf?earliest=-2d%40h&latest=now). This includes all complex scenarios of reading/writing the data. Vagabond scenario is key-value data with operations as get/put/delete by key, so we expect the latencies for this simple scenario will be less.

### **Availability Considerations** {#availability-considerations}

Availability of vagabond is very critical for core, especially for SOQL API use case.  Given that vagabond is not a SOR and API use case is only a cache of SOQL resultset, availability of service is more important than possible non persistence of data.  
HBase is a relatively complex system and few of the incidents resulted in downtimes of few mins to 1 hour \[TODO: add few references here\].  In addition, there can also be hbase get/put intermittent failures due to timeouts with hbase busy with compaction/running complex queries \[TODO: Validate this with hbase team\].

With this, it is preferable to continue running vagabond by storing data to CaaS when Hbase is down.  This mitigates the availability issues of vagabond when HBase is not available).   We have below two options in dealing with PUTs when hbase is down.

#### Option 1: Ignore the HBase write failures {#option-1:-ignore-the-hbase-write-failures}

Ignore the failures and depend only on the CaaS for that data. 

* GET requests will be failed if CaaS evicts the data.   
* GET requests will be failed if CaaS node is down.

Support for CaaS cluster in future will improve the availability further.

#### Option 2: Handle the HBase write failures {#option-2:-handle-the-hbase-write-failures}

Once HBase is up and running after downtime, then data can be pushed to HBase by reading from CaaS.   It is to be noted that the capacity is limited and CaaS itself can lose data, hence this will improve data durability but not mitigate all possible scenarios.  
Approach for copying the data from CaaS to HBase

1. Maintain a queue in CaaS to store the failed operations ( operation\_name \+ key ).  
2. When HBase operation failed or timed out, then append the operation to the queue.   
   Size of the queue is limited and we can start losing keys when HBase is down for longer duration.  
3. Schedule a background job(s) to replay the operations to HBase and cleanup the CaaS queue.

Since both CaaS and HBase are highly available services, so the scenario of both services going down at the same time is low. But if both are unavailable at the same time, then requests will fail.

### **Scalability** {#scalability}

With this new architecture, vagabond scalability is limited by CaaS scalability and HBase scalability.  Since both the services are scalable in the cluster size range as required by Vagabond, we should not see any scaling issues with this approach.  
Current spod/kingdom level usage data is below to give an idea of scale.  
[https://docs.google.com/spreadsheets/d/13TQcYnHGtWelFPmDATnn0X1I7MeQZ7\_f-VLElCe9v4c/edit\#gid=978487489](https://docs.google.com/spreadsheets/d/13TQcYnHGtWelFPmDATnn0X1I7MeQZ7_f-VLElCe9v4c/edit#gid=978487489)

### **Administration & Monitoring** {#administration-&-monitoring}

We will have a new scone service for performing the various administration and monitoring tasks.

1. Data copying from CaaS to HBase, once HBase is up after downtime.  
2. On demand cleaning of the data for a given orgid.  
3. Tasks related to cleanup & monitoring

For core use-cases these tasks will be performed from the app server using scheduled job.

### **Comparison of new architecture with current architecture** {#comparison-of-new-architecture-with-current-architecture}

|  | Current Architecture | New Architecture |
| :---- | :---- | :---- |
| **Storage** | Local Disk  | HBase |
| **Hardware** | 4 hosts per pod | Couple of nodes per kingdom/DC level for background jobs.  So, vagabond-ng becomes nearly a library with few reconciliation/cleanup jobs.For core use-cases, these jobs run from core itself. |
| **DR Support** | No | Possible to support as HBase supports  |
| **Kingdom/DC level support** | No | Possible to support as HBase supports  |
| **Security Compliance of Storage** | No | Possible to support as HBase supports  |
| **Latencies** | P90 latency for GET,PUT operation is approx 5ms | GET latencies will be less based on CaaS capacity. PUT latencies may be slightly higher as HBase latencies are higher.  |
| **Network usage from client** | 2x data as 2 replicas are written from client. | Still 2x as we write one copy to HBase and one copy to CaaS |

# **Storage Design** {#storage-design}

## **HBase Table design** {#hbase-table-design}

### **Schema** {#schema}

This table is used to store simple ungrouped key-value data.

| VG.SIMPLE\_DATA |  |  |  |  |
| :---- | :---- | :---- | :---- | :---- |
| KEY | ORG\_ID | CLIENT\_TYPE | VALUE | CREATED\_DATE |
|  |  |  |  |  |

This table is used to store the grouped key-value data with an index on GROUP\_ID column to perform GET/DELETE operations on GROUP\_ID

| VG.GROUP\_DATA |  |  |  |  |  |
| :---- | :---- | :---- | :---- | :---- | :---- |
| KEY | GROUP\_ID | ORG\_ID | CLIENT\_TYPE | VALUE | CREATED\_DATE |
|  |  |  |  |  |  |

Note: It is a tentative table design and will change based on the discussion with HBase team on possible [approaches](https://docs.google.com/document/d/1LgesWD_brHyoxkvFSdIJnBCf4kzFliGoZpDAOL08USk/edit#heading=h.ln4dn9xxfwi7). 

### **HBase Capacity** {#hbase-capacity}

HBase capacity needed is same as the At-rest data with current vagabond server.  
Current vagabond server is storing the data in Berkeley DB and Local disk across 4 nodes with 3 times replication.

* Below approach is used to calculate the At-rest data.

*At-Rest Data \= (Sum(Berkeley DB Size per host) \+ Sum(Disk partition Size per host))/3*

* Capacity listed in below document as per this approach  
  [https://docs.google.com/spreadsheets/d/13TQcYnHGtWelFPmDATnn0X1I7MeQZ7\_f-VLElCe9v4c/edit\#gid=978487489](https://docs.google.com/spreadsheets/d/13TQcYnHGtWelFPmDATnn0X1I7MeQZ7_f-VLElCe9v4c/edit#gid=978487489)  
* 2TB storage at DC level should be enough based on the current usage.

### **HBase TTL** {#hbase-ttl}

Use the per cell TTL of HBase. But this is not yet supported by Apache Phoenix.  
([https://issues.apache.org/jira/browse/PHOENIX-1335](https://issues.apache.org/jira/browse/PHOENIX-1335))  
Note: Yet to validate this with hbase team among various [options](https://docs.google.com/document/d/1LgesWD_brHyoxkvFSdIJnBCf4kzFliGoZpDAOL08USk/edit#heading=h.6a6ljooh64ct)

## **CaaS**  {#caas}

### **Data Types** {#data-types}

* Hash : For cursor scenario  
* byte\[\] : For all other scenarios

### **CaaS Capacity** {#caas-capacity}

Since CaaS is look-aside cache for improving GET request latencies, we should be good if we serve the 90% GET requests from the CaaS and remaining from the HBase. So we should store minimum amount of data in CaaS to serve the 90% GET requests.

Below approach is used to find the capacity needed based on the current usage for each pod.  
**Step 1: Find the duration to hold the data in CaaS**  
Calculate the time difference between the PUT and GET during the GET request processing. Take the p90 of these values over a period of a week per pod. If we store the data for this time span in CaaS, then approximately 90% of requests will be served from the CaaS.  
**Step 2: Calculate the size for the duration**  
Calculate the moving sum of PUT request key-value for the duration over a period of a week. Take the maximum moving sum value per pod. 

As per this approach, capacity captured per pod is listed in below document.  
[https://docs.google.com/spreadsheets/d/1DREYBJ9LLGYMxGHl9-S7BWk3OiFx8oaQTE\_dpbYcXNM/edit\#gid=246815201](https://docs.google.com/spreadsheets/d/1DREYBJ9LLGYMxGHl9-S7BWk3OiFx8oaQTE_dpbYcXNM/edit#gid=246815201)

### **CaaS TTL**  {#caas-ttl}

With the above approach, we can estimate the overall capacity needed at the DC/SPOD level.  
Since the CaaS capacity is limited, there is chance that one org/pod may consume more memory on it peak usage. To restrict the memory size for org/pod we have below options.

1. CaaS governance to throttle the memory sizes per tenant.  
2. Fixed TTL for each client type based on the historical usage pattern.  
3. Dynamically decide TTL for each client type,org/pod using the dynamic percentile calculation on the time difference between PUT and GET.

## **Migrating to new storage(CaaS+HBase)** {#migrating-to-new-storage(caas+hbase)}

Below are the options for smooth migration while switching the storage from current vagabond server to CaaS+HBase. Migration phase is 10 days which is the max TTL among the current client types. During this phase, we need to maintain the status quo.

**Option 1:** Write to new storage CaaS+HBase and read from CaaS+HBase first and then from Vagabond server for missing keys.  
	Pros: No extra latency during PUT calls  
	Cons: Extra latency during GET calls for old data  
**Option 2:** Write to both the storages and read from Vagabond server till migration phase  
	Pros: No extra latency during GEt calls.  
	Cons: Extra latency during PUT calls as we need to write to three storages.  
           Manually need to switch to new storage after migration phase.  
**Option 3:** Data copy from vagabond server to CaaS+HBase and switch to CaaS+HBase  
	Pros: No extra latencies during GET/PUT.  
	Cons: Data copying is complex and tricky as the data is getting added at high rate.

**Conclusion:** Option2 is chosen for smooth migration by doing concurrent write to two stores.

Below are the 3 stage migration by analyzing the stats at each stage and take a decision to proceed to next stage or go back to previous stage.

# Step1 : Vagabond → Vagabond-Dual ( Primary: Vagabond) {#step1-:-vagabond-→-vagabond-dual-(-primary:-vagabond)}

* PUT/DELETE will happen concurrently to both the stores.  
* GET will happen on vagabond server

	  
	Step2: Vagabond-Dual ( Primary: Vagabond) → Vagabond-Dual ( Primary: HBase)

* PUT/DELETE will happen concurrently to both the stores.  
* GET will happen on HBase server and in case of failures/missing data fallback call to vagabond server

	Step3: Vagabond-Dual ( Primary: HBase) → Vagabond-NG

* PUT/DELETE will happen to only new store  
* GET will happen on HBase server 

# **Client Types** {#client-types}

Below are the current client types in core using the vagabond to store the temporary data.

## **Cursor use-cases** {#cursor-use-cases}

Around 94% of requests to the vagabond are from the cursor use cases.  These are latency sensitive and are primary use case for vagabond

| Client Type | Max payload size | Operations | Bulk Operations  | Description |
| :---- | :---- | :---- | :---- | :---- |
| API | 180KB | GetPut | BulkPut | Stores SOQL cursor data and also external query identifier |
| APEX\_BATCH | 15MB | GetPutDelete |  | Stores the apex batch state/limit data and also batch cursor data |
| SYNC\_STATE | 5MB | GetPutDelete | BulkDelete(For Testing) | Stores the snapshot cursor ids |

## **Blob use-cases** {#blob-use-cases}

Around 5% of requests are from these use cases. Due to the high payload sizes, these client types will cause heavy memory usages in CaaS and HBase and may affect the overall performance.  Also these are non-sensitive to millisecond level latencies. So these client types **will not be moved to Vagabond-NG**. 

| Client Type | Max payload size | Operations | Bulk Operations  | Description |
| :---- | :---- | :---- | :---- | :---- |
| APEX\_CSI | 200MB | GetPut |  | Stores the processed apex CSI trace data. No explicit deletes from client code. TTL is 2 hours |
| APEX\_DEBUGLOG | 15MB | GetPutDelete |  | Stores CPU profiler snapshot data Stores the various debug logs from apex |

## **Miscellaneous use-cases** {#miscellaneous-use-cases}

Around 1% of requests are from the below use cases. 

| Client Type | Max payload size | Operations | Bulk Operations  | Description |
| :---- | :---- | :---- | :---- | :---- |
| DEPLOY | 2.5MB | GetPutDelete |  | Stores the code coverage and state information |
| REPORT\_RESULTS | 1MB | GetPutDelete |  | Used for transient storage of report results.  |
| PLATFORM\_MONITORING | 30KB | GetPutDelete | BulkGet |  |
| PACKAGE\_UPLOAD | 8KB | GetPut |  | Stores the entity names strings  |
| PACKAGE\_VALUE\_UPDATE | 13KB | GetPut |  | Used for storing data related to Feature Parameters for packaging |

# **Related Documents** {#related-documents}

1. [Brief history of vagabond](https://docs.google.com/document/d/1_yd-Q4zBEFGDrvZUAhK-tac5ZpfSFAtkrp3amlxv_rY/edit#heading=h.53mb8ll7qmde)  
2. [Vagabond-NG HBase Design](https://docs.google.com/document/d/1LgesWD_brHyoxkvFSdIJnBCf4kzFliGoZpDAOL08USk)  
3. [Vagabond-NG Technical Design](https://docs.google.com/document/d/1fcbLG4Xz8LqhpSsNHpsUTLOLCQWk0_cRkwDjAJuJysI)

[image1]: <data:image/png;base64,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>