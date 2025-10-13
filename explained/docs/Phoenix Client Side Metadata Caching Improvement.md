# Phoenix Client Side Metadata Caching Improvement

The purpose of this document is to describe the Phoenix client side metadata caching issues and how to address them.

# Background

The ConnectionQueryServices interface is designed to 

* create and manage connectivity to the underlying HBase cluster,
* access plugable query services (optimizer, executor, and memory manager),
* provide an internal client interface to access the Phoenix metadata server to create and delete the HBase objects backing the Phoenix objects exposed through the SQL API (Tables, Sequences, Functions, Statistics, etc), and update their properties, and 
* wrap the HBase client API for accessing HBase tables and the HBase metadata, including table descriptors, table region boundaries and locations, and column descriptors.

ConnectionQueryServicesImpl (CQSI) is the main implementation of the ConnectionQueryServices interface. 

CQSI maintains a client-side metadata cache, i.e., schemas, tables, and functions, that evicts the last recently used table entries when the cache size grows beyond the configured size. PMetaData is the interface for this cache and PMetaDataImpl implements the interface using PMetaDataCache. PSynchronizedMetaData wraps the PMetaDataImpl to serialize the access to the cache. The latestMetaData field of CQSI holds this cache object.

Each time a Phoenix connection is created, the client-side metadata cache maintained by the CQSI object creating this connection is cloned for the connection. Thus, we have two levels of caches, one at the Phoenix connection level and the other at the CQSI level. 

When a Phoenix client needs to update the client side cache, it updates both caches (on the connection object and on the CQSI object). The exact reason behind the choice of cloning the entire cache for each connection is not clear but it is likely to eliminate lock contention on the metadata cache maintained by CQSI objects during read operations. 

The Phoenix client attempts to retrieve a table from the connection level cache. If this table is not there then the Phoenix client does not check the CQSI level cache, instead it retrieves the object from the server and finally updates both the connection and CQSI level cache. On the server side, the coproc called MetaDataEndpointImpl is responsible for maintaining metadata held in the system catalog table. 

Both client side caches are implemented using the same class PMetaDataImpl except that this class is wrapped by PSynchronizedMetaData to provide synchronized access to the cache on the CSQI object. Since Phoenix connections are not thread safe and a Phoenix connection is supposed to be accessed by a single thread at given time, such synchronization is not necessary for the cache on the connection object.

PMetaDataCache provides caching for tables, schemas and functions but it maintains separate caches internally, one cache for each type of metadata. The cache for the tables is actually a cache of PTableRef objects. PTableRef holds a reference to the table object as well as the estimated size of the table object, the create time, last access time, and resolved time. The create time is set to the last access time value provided when the PTableRef object is inserted into the cache.  The resolved time is also provided when the PTableRef object is inserted into the cache. Both the created time and resolved time are final fields (i.e., they are not updated).  PTableRef provide a setter method to update the last access time. PMetaDataCache updates the last access time whenever the table is retrieved from the cache.  The LRU eviction policy is implemented using the last access time. The eviction policy is not implemented for schemas and functions.

The configuration parameter for the frequency of updating cache is phoenix.default.update.cache.frequency. This can be defined at the cluster or table level. When it is set to zero, it means cache would not be used.

# Problem Statement

Obviously the eviction of the cache is to limit the memory consumed by the cache. The expected behavior is that when a table is removed from the cache, the table (PTableImpl) object is also garbage collected. However, this does not really happen because multiple caches make references to the same object and each cache maintains its own table refs and thus access times. This means that the access time for the same table may differ from one cache to another; and when one cache can evict an object, another cache will hold on the same object. 

Although individual caches implements the LRU eviction policy, the overall memory eviction policy for the actual table objects is more like age based cache. If a table is frequently accessed from the connection level caches, the last access time maintained by the corresponding table ref objects for this table will be updated. However, these updates on the access times will not be visible to the CQSI level cache. The table refs in the CQSI level cache have the same create time and access time. 

Since whenever an object is inserted into the local cache of a connection object, it is also inserted the cache on the CSQI object, the CQSI level cache will grow faster than the caches on the connection objects. When the cache reaches its maximum size, the newly inserted tables will result in evicting one of the existing tables in the cache. Since the access time of these tables are not updated on the CQSI level cache,  it is likely that the table that has stayed in the cache for the longest period of time will be evicted (regardless of whether the same table is frequently accessed via the connection level caches).  This obviously defeats the purpose of an LRU cache.

Another problem with the current cache is related to the choice of its internal data structures and its eviction implementation. The table refs in the cache are maintained in a hash map which maps a table key (which is pair of a tenant id and table name) to a table ref. When the size of a cache (the total byte size of the table objects referred by the cache) reaches its configured limit, how much overage adding a new table would cause is computed. Then all the table refs in this cache are cloned into a priority queue as well as a new cache. This queue uses the access time to determine the order of its elements (i.e., table refs). The table refs that should not be evicted are removed from the queue, which leaves the table refs to be evicted in the queue. Finally, the table refs left in the queue are removed from the new cache. The new cache replaces the old one. It clear that this is an expensive operation in terms of memory allocations and CPU time. The bad news is that when the cache reaches its limit, every insertion would likely cause an eviction and this expensive operation will be repeated for each such insertion.

Since Phoenix connections are supposed to be short lived, maintaining a separate cache for each connection object and especially cloning entire cache content (and then pruning the entries belonging to other tenants when the connection is a tenant specific connection) are not justified. The cost of such a clone operation by itself would offset the gain of not accessing the CQSI level cache as the number of such accesses per connection should be small because of short lived Phoenix connections. 

Also the impact of Phoenix connection leaks, the connections that are not closed by applications and simply long lived connections will be exacerbated since these connections will have references to the large set of table objects.

At Salesforce, the cache update frequency is set to zero for all the tables except one, VAGABOND.KEY_VALUE_DATA.  Although Phoenix retrieves the table objects from the server for the tables with zero cache update frequency, the cache entries are still inserted to both caches even for these tables. This means these tables occupy the cache space unnecessarily and reduce its effectiveness. 

By now, it should be clear to the reader that this design is overly expensive and does not serve its purpose. 

# Solution

As implied by the previous section on the problem statement, it is an overkill to use a separate full cache for Phoenix connections and the current cache design is inefficient and expensive. To address all these issues, we propose eliminating connection level caching and leveraging a well-known thread safe efficient caching library which is Guava Cache from Google to implement the CQSI level cache. Please note that Phoenix already uses Guava Cache to implement the cache of CQSI objects. 

The current cache implementation uses the total memory footprint of the table objects in the cache to determine when to evict. Guava Cache supports this type of use cases by allowing a weight value for each cache entry and the maximum total weight for the cache to be used to determine when to evict.

Currently the tables with zero cache update frequency are retrieved from the server each time they are accessed for a query or mutation even within the same Phoenix connection. After every retrieval from the server, the old cache table ref is removed from the cache and the new one is inserted unnecessarily.  Another obvious improvement is that tables with zero cache frequency should not be inserted into the cache.

Finally, it is clear that we do not take advantage of the client side metadata caching at Salesforce enough as only one user table is configured to use the cache. Since the metadata of the existing the tables are updated very infrequently for most of the tables, these tables should take advantage of the client side caching. This will reduce the load on the MetaDataEndpointImpl coproc and in its region server greatly.















