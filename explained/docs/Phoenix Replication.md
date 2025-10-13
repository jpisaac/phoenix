# Phoenix Replication

Contributors: [Kadir Ozdemir](mailto:kozdemir@salesforce.com)  
Updated: Mar 11, 2024

This document lays out the design for a new feature called *Phoenix replication* for replicating Phoenix objects (table, views and tenants) from one cluster to another. This design leverages max lookback and uncovered global indexes. 

# Use Cases and Value-adds

Although both Phoenix replication and CDC use the same Phoenix features that are max lookback and uncovered global indexes, they have different use cases and value-adds. This section explains the use cases and value-adds for Phoenix replication. For CDC, please [Phoenix Native Change Data Capture](https://docs.google.com/document/d/1Dk6UDcYj9GAC25hG_83PPQjbURjXmHIMVqIckzkwa2s/edit#heading=h.9b8r7qa2nvxe).

## Use Cases

The main use cases for Phoenix replication include data migration, disaster recovery, backup, and offloading data processing. 

### Data Migration

In this use case, data for a set of Phoenix objects from a source cluster  is migrated to a target cluster while the data is changing on the source cluster. When the data on the target is synchronized with the data on the source cluster, the clients can switch using the data on the target cluster. This switch happens almost instantaneously so that the impact of the data migration on the availability of the data would be minimal without any data loss. 

### Disaster Recovery

This is similar to the data migration with the following differences. 

* Data from all objects can be replicated.  
* Failover (switch)  from one cluster to the other can happen any time (when the disaster strikes).   
* Replication can be bi-directional (for active-active deployments).  
* There can be some data loss due to asynchronous replication.

### Backup

In backup use cases, data is backed up to a remote system. Backup images can be generated from the data on the source system or the data on the target cluster after the data to be used for backup images is migrated to the target system.

### Near Real-time Offloading Data Processing

In this use case, the processing data on the source cluster needs to be done on a remote system in near real-time. This is done to reduce the load of this data processing on the source cluster as the resources (I/O, CPU, or memory) required for processing may not be freely available on the source cluster or the clients of the source cluster. This requires pushing the changes on the data to the target cluster and processing the data there. The result of the data can be pushed back to the source cluster. 

## Value-adds

### Live Data Replication

This is for capturing changes to a given table (or updatable view) as these changes happen in near real-time and replicating them to another cluster. Phoenix replication will capture these changes and replicate them in real-time. A change can be inserting a new row, updating one or more columns of a table for an existing row, or deleting a row. Phoenix replication will replicate these changes to the target cluster in the order of their timestamp. 

### Time-range Replication

In addition to replicating changes while they are happening in near real-time, Phoenix replication can go back in time and replicate change for a given time range. This separates Phoenix replication from the simple log based replication which replicates a given change only once. Phoenix can be used for replicating data for recovery purposes and near real-time remote data processing. 

Examples of data recovery use cases include generating backups and storing them on a remote system and recovering from data corruption or data loss incidence on the destination, processing the changes multiple times possibly at different times in future. 

Near real-time remote data processing use cases is to offload the processing of data in the source cluster to a remote system that has its own cluster.

### Initial-copy Replication

Another difference is that Phoenix replication can also be used for initial copy as it captures changes for the entire lifetime of a given table in an efficient way. It captures every change within a predefined look back period and captures only at most N versions of rows beyond this look back period. For Phoenix use cases, N almost always is equal to 1 and can be configured to something else.

### Multi-tenant Replication

Phoenix supports multi tenancy. Phoenix replication can be configured for a subset of tenants in a table, instead of replicating the entire table. For example, initial copy, time-range replication and live data replication with multi-tenancy can be an attractive solution for migrating an org from one cluster to another.

### Network Bandwidth Efficient Replication

One more important benefit of Phoenix replication is its ability to send data using an efficient wire format as Phoenix replication does not need to tag data with metadata on the wire. In HBase data is stored in cells and a cell is a self-describing structure that includes metadata including the row key, family, column qualifier, and timestamp in addition to the data of the cell. HBase replication and copy operation have to transfer cells while Phoenix replication transfers only data.

In addition to the efficient wire format, Phoenix replication does not replicate index table mutations, instead these mutations are generated on the target system.

### Replication and Transformation

Phoenix replication does not require the table in the destination cluster to have the same name or schema as the source does. The data will be written to the specified table and also transferred to the right format at the destination if the schema at the destination is different.

# High Level Design

Phoenix replication leverages the existing Phoenix features more specifically uncovered global index and max lookback features to capture the changes. The max lookback feature retains recent changes to a table, that is, the changes that have been done in the last n days typically. This means that the max lookback feature already captures the changes to a given table. 

To deliver the changes in the order of their timestamp order, Phoenix replication uses an uncovered global index on the HBase mutation timestamp. This index is uncovered since the changes are already retained in the data table by the max lookback feature. 

## Architecture

The diagram below shows the architecture for Phoenix replication. 

*Phoenix Source* is the replication end point for the source cluster and is responsible for establishing connection with its counterpart on the target system, *Replication Sink*. 

*Phoenix Replication* embeds Phoenix client and implements the client functionality for Phoenix replication. On the source side, it is responsible for scanning the uncovered global index table rows for a given timestamp range using a raw scan of the HBase API without going through the typical Phoenix client stack. This scan includes a scan attribute to indicate that this scan is for Phoenix replication. On the destination, it translates the write format into the HBase mutations to be written on the target table.

*Replication Region Scanner* is the Phoenix server class that scans the index table and joins the index table rows with the data table rows using the existing uncovered index server code. 

Note the common code between CDC and Phoenix replication will be factored out and shared between these features on both client and server side. The Phoenix replication feature which does not prepare pre and post images will be simpler and leaner than CDC for capturing and preparing changes. For example, unlike CDC,  Phoenix replication would not represent the changes using JSON and would not need a separate table type (i.e.,CDC Table). The rest of the high level design section explains the replication wire format and metadata, and replication specific design.

## Replication Wire Format

The changes are retrieved using a specific timestamp range for a given table or an updatable view. The changes are serialized using ProtoBuf. Each retrieved change includes two sections. The first section is for the (static) columns of the table specified in the table schema. The second section is optional and for dynamic columns if the table has any dynamic columns.

The static column section includes the change timestamp, the bitmap to specify the static columns included columns in the change, and an array of serialized values for these columns. The dynamic columns section includes an array of (dynamic) cells.

## Orchestration and Recovery of Replication Tasks

*Replication Host* shown in the architecture diagram can be a physical host, virtual machine, kubernetes pod, etc. In Falcon, kubernetes pods will be replication hosts.  Each replication host can replicate multiple Phoenix objects which are tables, views, or tenants.  

The replication host will execute *replication tasks*. A replication task will be replicating a Phoenix object for a specified time range. A time range can be open ended too. For example, an initial copy task can be specified with the time range \[INITIAL, \<current time\>\] and a live replication task with the time range \[\<a time in the past\>, PRESENT\].

Replication tasks will be stored on a Phoenix table called PhoenixReplicationSourceTable. This table is used by the source of replication tasks. A watermark (i.e., the timestamp of the last shipped record) is stored on the same table for each replication task. This table will also be used to arbitrate which replication task will be assigned to which replication host.

Replication hosts scan this table, claim the replication tasks using Phoenix atomic upsert operations (that are atomic conditional updates), and then update the tables with the status of the replication task and the watermark for the replication task. 

Phoenix row timestamps on the table records can be used as the timestamp of the last heartbeat from replication hosts. Heartbeat timestamps are used to detect failed hosts and thus to failover an (orphan) task from one host to another or the same host if the host comes back. This failover is done by a replication source host by claiming the orphan task.

Another table called PhoenixReplicationSinkTable is used by replication sinks. This table includes records for metadata for replication tasks including source object, target object if it is different from the source object and the transformation specification if required.

The configuration information about replication source and sink is stored in a configuration file. The replication source retrieves the sink end-points from this file. Replication sources uniformly distribute the load to the replication sinks by randomly connecting to the sink end-points.

