# Accessing Argus Data Using Phoenix

Contributors: [Kadir Ozdemir](mailto:kozdemir@salesforce.com)[Viraj Jasani](mailto:vjasani@salesforce.com)[David Manning](mailto:david.manning@salesforce.com)[Vincent Poon](mailto:vincent.poon@salesforce.com)  
Updated: Feb 14, 2025

There are many benefits of making Argus metrics available through Phoenix. This allows Huron to access Argus clusters using a Trino-Phoenix connector and turn existing Argus clusters into metrics hot stores. Huron then can federate queries over Argus and S3, and so provide a unified SQL access to data on the hot (Argus) and cold (S3) store. In addition to this, Phoenix features including paging and push down aggregations will be immediately available for these queries and make these queries more efficient and less noisy. The other advanced features including indexing and change data capture can be leveraged in future when Argus data is ingested via Phoenix.

Argus uses openTSDB as its timeseries database. Both openTSDB and Phoenix use HBase as their backing store. However, their data model and format are not compatible, and thus Phoenix cannot directly query the Argus data in HBase. The purpose of this design document is to devise a Phoenix schema and computationally efficient server side in-memory transformation to make existing Argus data queryable by Phoenix.

The pragmatic approach adopted here prioritizes minimal changes to facilitate a smooth and adaptable transition and evolution. OpenTSDB utilizes a compact and efficient representation of a metric store, condensing one hour of metric data into a single HBase row. All metric data attributes are encoded using tokens (UIDs for tag key-values) within the row key. Consequently, these tags undergo evaluation and filtering once per hour of data rather than for each individual data point within the hour. Combining this with the Phoenix query engine and server-side push-down processing yields a potent time-series database solution that transcends the mere notion of substituting OpenTSDB with Phoenix.

# Argus Data Model and Format

The format of the row key of the HBase table for metrics is as follows.

\<metric\_uid\>\<timestamp\>\<tagk1\>\<tagv1\>\[...\<tagkN\>\<tagvN\>\]

An Argus tag (that is an openTSDB tag) is a key-value pair where both key and value are a UID of type 6 byte binary. Each metric has one or more tags. These tags identify additional attributes for each data point such as device, host, pod etc., typically to identify the source of the metric data point. A time series is a series of numeric data points of a particular metric uid and one or more tags over time. Essentially, the array of key-values encoded in the row key shown above forms a map data type. Phoenix currently does not support a map data type. The closest would be JSON or BSON data. However, neither a JSON or BSON data type can represent a map where keys are binary as in these data types the keys must be of type string. 

Each row includes multiple columns. Each column holds a metric data point. The column name encodes the delta timestamp from the timestamp component of the row key.  Each row holds an hour worth of metric data. The number of data points and column names can change from one row to the next. If we want to generate this data layout on disk using a Phoenix schema, the columns should be dynamic columns. Dynamic columns are not specified in a Phoenix schema (as they are generated on the fly), and thus the Phoenix query optimizer cannot formulate (at least currently) any efficient query plan for them. 

The above data model gives users the ability to add and remove tags dynamically and freely. As explained [here](https://salesforce.quip.com/t6fUADa1qu6n), this freedom causes inefficient uses of compute, memory and IO resources and thus causes performance issues.

### TSDB Query Semantics and Construction

Note that because the row key has only UIDs, and no names of metrics or tags, the HBase client and regionserver can only operate on UIDs. Today an Argus query must translate all names into UIDs before the query is sent to the HBase client. This service is performed in openTSDB. An ArgusQL query with wildcards in metric names or tag values must translate those wildcards into literal names before sending to openTSDB, which will then translate the names to UIDs. For this feature, Argus maintains an ElasticSearch cluster which updates itself during metric ingestion with the most recently observed combinations of metrics and tags. Any wildcard other than “all tag values for a given tag” (i.e. pod=\*) must fully expand into all known names which match the wildcard. This can create arbitrarily complex filters with long lists of UIDs to be sent to HBase.

A tsdb query is translated into an HBase Scan request with a RegexStringComparator for selecting the appropriate row keys that contain the desired tags. As an example, a query for metric\_name{k8s\_pod\_name=\*,k8s\_cluster=hbase1a} will find all matching rows that have a rowkey [matching regex](https://git.soma.salesforce.com/ArgusMonitoring/opentsdb/blob/52347b9572deea0aaf3f04f0d54408dab32eb66b/src/query/QueryUtil.java#L98-L101): ^.{10}(?:.{12})\*{k8s\_cluster\_tagk\_uid}{hbase1a\_tagv\_uid}(?:.{12})\*{k8s\_pod\_name\_tagk\_uid}.{6}(?:.{12})\*$

where:

* ^.{10} represents the first 6 bytes for metric\_name\_uid and 4 bytes for timestamp\_hour, but are unnecessary to validate in the regex because start\_key and end\_key in the scan range will account for this.  
* (?:.{12})\* represents optionally match any 12 bytes (any 6 tagk and 6 tagv that we are not interested in matching in the regex filter.)  
* {k8s\_cluster\_tagk\_uid}{hbase1a\_tagv\_uid} are the 12 bytes representing the tagk and tagv to match.  
* .{6} represents any match for tagv for k8s\_pod\_name, following the 6 bytes for the k8s\_pod\_name tagk.

# Huron Metric Schema

There are two types of Argus tags, standard and custom. Each of the standard tags is represented as a separate column in the Trino table schema in Huron. The custom tags are represented as key-value pairs in a MAP type column called tags in this schema. The Huron metric table has a separate row for each metric data point. Having a separate column for standard tags and a separate row for each data point allows Huron to generate more optimized query plans on S3 than Argus on OpenTSDB. The Huron metric schema can be found [here](%20https://bdmpresto-superset-server.sfproxy.uip.aws-esvc1-useast2.aws.sfdc.cl/superset/sqllab/?savedQueryId=15487).

# Design

One of the non-functional objectives of the design presented in this document is to generate an optimal query plan for the Argus data model and format. Here an optimal query means the query uses the CPU, memory, I/O and network resources optimally. It achieves this objective by satisfying the following properties:

1. For a given region of the HBase metric table, all filtering, down sampling, grouping and aggregation, etc. are executed on the region server for this region.  
2. It uses minimum number of comparisons to filter out metric table rows for all types of queries

The other non-functional objective is to prevent noisy neighbor problems while executing the queries. This is achieved by employing the Phoenix server paging feature with fine-granular paging.  
   
The functional objective of the design is to allow Huron to federate the metric data in Argus and S3. For this purpose, the schema presented to Trino by a custom Phoenix connector will be almost identical to the existing Huron metric schema. The differences if any will be resolved by casting within the view schema for the federated data. This schema is not stored in SYSCAT. It is hard coded within the custom Trino-Phoenix connector.  

The actual Phoenix table that will be created over existing the Argus metric table will have the following schema:

metric BINARY(6),  
timestamp UNSIGNED\_INT,   
tags BINARY(6) ARRAY\[\],  
value UNSIGNED\_FLOAT,  
CONSTRAINT PK PRIMARY KEY(metric, timestamp, tags)

```
CREATE TABLE IF NOT EXISTS "tsdb" (metric_uid BINARY(6) NOT NULL, timestamp UNSIGNED_INT NOT NULL, tags_uid BINARY(6) ARRAY[] NOT NULL, v UNSIGNED_DOUBLE, CONSTRAINT pk PRIMARY KEY(metric_uid, timestamp, tags_uid)) DATA_BLOCK_ENCODING='FAST_DIFF', TTL=3888000, COMPRESSION='SNAPPY', NORMALIZATION_ENABLED='true', NORMALIZER_TARGET_REGION_SIZE='5200', UPDATE_CACHE_FREQUENCY=172800000;
```

The above Phoenix schema generates a row key that is compatible with the Argus metric table in HBase. This schema generates the same row key image of the underlying HBase table for metrics except the timestamp component of the row key. In the Argus metric table, the timestamp is specified at hour granularity as a row corresponds to an hour of metric data. In the above schema, timestamp corresponds to the actual timestamp of the data point.

The row layout generated by this schema is not compatible with the HBase table. This is because each row in HBase holds multiple data points while this schema defines a single data point per row. This means that we need to convert a multi data point (column) row to N single data point rows. 

Converting a row with multiple columns (one column per data point) to a row with a single column will be done on the server side by the region scanner called Row Conversion Region Scanner as shown in the below diagram. With this conversion, the delta timestamp in the column qualifier will be added to the timestamp component of the row key in addition to dividing a multi column row with N columns into N single column rows. The columns that are outside the time range of the query are dropped here. This happens when the start or end time of the query is not at the hour boundary.

Before we dive into Phoenix function definitions that can help convert UID to Name and vice versa, let’s understand the “tsdb-uid” table schema. It’s important to note that the “tsdb-uid” table does not need to have corresponding table representation in Phoenix as it needs to be accessed using HBase APIs directly. The query pattern on tsdb-uid table is mostly single row point lookups.

tsdb-uid table definition:

```
tsdb-uid, {TABLE_ATTRIBUTES => {METADATA => {'DISABLE_TABLE_SOR' => 'true', 'hbase.store.file-tracker.impl' => 'DEFAULT'}}}
                                                                                                                       
COLUMN FAMILIES DESCRIPTION

{NAME => 'id', INDEX_BLOCK_ENCODING => 'NONE', VERSIONS => '1', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', TTL => 'FOREVER', MIN_VERSIONS => '0', REPLICATION_SCOPE => '0', BLOOMFILTER => 'ROW', IN_MEMORY => 'false', COMPRESSIO
N => 'SNAPPY', BLOCKCACHE => 'true', BLOCKSIZE => '65536 B (64KB)'}                                                                                                                                                                                  

{NAME => 'name', INDEX_BLOCK_ENCODING => 'NONE', VERSIONS => '1', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', TTL => 'FOREVER', MIN_VERSIONS => '0', REPLICATION_SCOPE => '0', BLOOMFILTER => 'ROW', IN_MEMORY => 'false', COMPRESS
ION => 'SNAPPY', BLOCKCACHE => 'true', BLOCKSIZE => '65536 B (64KB)'}
```

The column family “id” contains the mapping from Name to UID. Depending on the type of UID, it can contain any of the three column qualifiers:

| CF:CQ | Cell Value |
| :---: | :---: |
| id:metrics | The metric UID for the given metric name in rowkey |
| id:tagk | The tag key UID for the given tag key name in rowkey |
| id:tagv | The tag value UID for the given tag value name in rowkey |

The column family “name” contains the reverse mapping from UID to Name. Depending on the type of UID, it can contain any of the three column qualifiers:

| CF:CQ | Cell Value |
| :---: | :---: |
| name:metrics | The metric name for the given metric UID in rowkey |
| name:tagk | The tag key name for the given tag key UID in rowkey |
| name:tagv | The tag value name for the given tag value UID in rowkey |

As in the case for Huron schema, the above schema has a separate row for each metric data point. The difference is that standard tags along with custom tags are encoded as an array of UIDs. In the absence of a map data type in Phoenix currently, tags (array of key-value pairs) will be represented as an array of UIDs such that zero and even offset elements are the keys and odd offset elements are the values. We need to introduce the following built-in functions

* FIND\_TAG\_VALUE(*UID*) → *UID* : Takes the name of a tag key in UID (that is 6 byte binary)  and returns the corresponding tag value in UID on a given row.  
* GET\_UID(*string, string*) → *UID* : Takes the first argument as the name of a tag key or tag value in string and returns the UID for it. The second argument represents the type of the UID to be returned, it can contain values: “metric”, “tagk” or “tagv”, representing “metric UID”, “tag key UID” and “tag value UID” respectively.  
* GET\_NAME(*UID, string*) → *string* : Takes a UID and returns the corresponding name. This is the reverse of GET\_UID(). The first argument contains UID in the Binary format. The second argument represents the type of the UID to be returned, it can contain values: “metric”, “tagk” or “tagv”, representing “metric UID”, “tag key UID” and “tag value UID” respectively.  
* REGEXP\_LIKE(*string*, *pattern*) → *boolean* : Evaluates the regular expression pattern and determines if it is contained within the string.

   
A given query may include partial tag keys or values that are specified by the SQL LIKE operator or Trino regexp function. Obviously such partial strings cannot be directly mapped to their UIDs. In this case, the UIDs for tag values from the row key need to be mapped to strings and the LIKE or regular expressions need to be evaluated on these strings.

For example, a Trino SQL statement may include the first WHERE clause below and the Trino-Phoenix connector will translate it to the second WHERE clause below.

```
WHERE REGEXP_LIKE(‘pod’, ‘region*’)
WHERE REGEXP_LIKE(GET_NAME(FIND_TAG_VALUE(tags_uid, GET_UID(‘pod’, 'tagk'), 'tagv'), “region*”) 
```

Similarly the following WHERE clauses with the LIKE operator are the equivalent to the above WHERE clause

```
WHERE 'pod'  LIKE 'region%'
WHERE GET_NAME(FIND_TAG_VALUE(tags_uid, GET_UID('pod', 'tagk')), 'tagv') LIKE 'region%'
```

This means we need a translation between Huron and Phoenix schema. This translation will be done in a custom Trino-Phoenix connector as shown in the following diagram.

Converting tag value UIDs to strings and then evaluating regular expressions on these strings will be done by the above Phoenix built-in functions that will be invoked by HBase custom filters included in Phoenix. This approach is radically different from but better than what the Argus client does. The Argus client enumerates all UIDs for a given regular expression and generates an HBase OR filter including all these UIDs. This means if a regular expression maps to N UIDs, each UID in the row key is compared against up to N UIDs whereas the approach of this design just executes the regular expression on the string corresponding to the UID instead of doing this up to N 6 byte comparison. 

The custom Phoenix connector for Trino is a modified version of the open source Phoenix connector such that it has a specific logic for the Argus metric table. This connector compiles the query to generate a query plan for the metric table. Its main purpose is to generate correct splits for a given query and translate the result of a query to Trino Java objects. The connector also returns a table schema that is compatible with the Huron schema if not identical. 

In order to generate the splits for a given query, the custom connector maps the pair of scope and metric column values to corresponding UID using the UID table in HBase. It also converts the time range of the query to the corresponding hourly time range. Using the metric UID and hourly time range, it forms the row key prefix range for the Argus metric table in HBase. This row key prefix range is used to identify the table regions to query and thus the splits for Trino.

The connector is also responsible for identifying the Argus cluster for a given query. If the query includes the FI/DC for the query can be determined from the query then the corresponding Argus cluster is used for the query. Otherwise, the query is executed in all Argus clusters in parallel.

As mentioned above, the actual compilation and optimization of a query is done within the connector. The connector rewrites the query using built-in functions, and compiles and optimizes the new query using Phoenix client library. The resulting scan is used to construct the server side Phoenix stack. 

The Argus coprocessor injects the Argus specific region scanner shown in red in the above diagram to the Phoenix stack.

# Potential for Other Applications

The design above discusses a solution to a concrete problem: providing a hot store of metrics in Huron-Trino via Phoenix queries to the existing HBase data store.

There have been many thoughts and discussions about scalability challenges in the Argus-openTSDB-HBase integration. ([sample references](https://salesforce.quip.com/pM8aAnop6KNK#temp:C:OYbad50961a117d44e083bc05d3a)) Proposed solutions include openTSDB renovation, Phoenix, and a replacement data store. One novel feature of this document’s design is that it allows a new query paradigm on existing Argus openTSDB data stored in HBase. If this is possible, it allows for direct performance comparison of problematic queries from both openTSDB and Phoenix. It provides a theoretical possibility for a query transition path without new clusters or data duplication. This would provide faster time to prototypes and insights, with cheaper engineering and financial costs.

We find in production today that existing openTSDB regex query patterns are expensive to evaluate in the regionserver, and we can exhaust all available CPU cycles. With control of the query execution, more predicate pushdowns are possible. With direct knowledge of the row key composition, more efficient filters could be applied nearer to the storage. Wildcard expansion could be done near the storage in a way that could significantly reduce the reliance on ElasticSearch for query compilation.

Reducing the dependency on ElasticSearch can have other benefits too. It is possible for ElasticSearch to be out of sync with the storage in HBase, for various reasons. So an Argus query for tags pod=\* and a query for tags pod=regionserver\* may return different results, even if all values for pod start with regionserver. Applying wildcards at the storage layer, instead of expanding wildcards with all known literal values from ElasticSearch metadata, has the potential to reduce inaccurate results, and simplify some queries. The effect is even more pronounced in cases with ephemeral tags in wide metrics with high cardinality in the tags, such as cadvisor metrics for k8s\_pod\_name for deployments, where each pod’s name is randomly generated.