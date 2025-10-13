# Distributed File System with Scalable Namespace

Contributors: [Kadir Ozdemir](mailto:kozdemir@salesforce.com)   
Updated: May 13, 2025

# Namespace Scalability Problem

Well-known physical or virtual distributed file systems (such as HDFS and Alluxio) scale well the storage capacity by employing a large number of data nodes where large file blocks are stored in local file systems of these data nodes but struggle with scaling their namespace.

The small file problem is a well-known issue that the open source distributed file systems suffer from. When a distributed file system stores small files, it may need to keep track of a large number of small files in its metadata (namespace). Namespace is a hierarchical structure and accessed and updated very frequently. The slicing and distributing of this frequently accessed and modified structure while preserving its integrity and performance is a challenging problem. 

Typically, in these highly scalable large-file file systems,  namespace is stored in a single node, called name node, where the entire namespace is kept in the memory of a name node. Therefore, the number of files stored in these types of file systems is limited with the size of the name node memory. 

# Solution

This invention provides a unique approach where only the root and inner nodes (i.e., directories) of the hierarchical namespace structure are maintained centrally and the leaf nodes (i.e., files) are maintained within the local file systems of data nodes. By doing so, it can reduce the size of the centrally stored metadata by several orders of magnitude while still preserving the integrity and performance of the metadata.  

This invention directly addresses the namespace scalability problem by leveraging the local file systems for storing not only file data but also namespace. It scales namespace and data storage using a unified approach and simplifies the overall solution. 

This invention maps  the files of the distributed file system to the files of the local file systems it leverages to store files. The local file systems that come with known operating systems such as Linux and Microsoft Windows are  highly performant, trusted, and  battle tested file systems. Mapping user files directly to local file system files, this invention not only eliminates maintaining the file metadata and file space management but also leverages the data integrity, performance and caching capabilities of these file systems.

This invention introduces the concept of containers which are internally used directories in local file systems and are used to hold metadata and data for user files. There are two types of containers, link and data. The link containers hold the metadata for files and data containers hold the data files. Containers are leaf directories in local file systems. Thus the invention is called Distributed Container File System (DCFS).

DCFS is a distributed file system built over local file systems that are mounted on nodes called data nodes. The namespace is also distributed over data nodes, and only the high level organization of the namespace is stored in name nodes. One of these name nodes is the primary name node and the others are the secondary name nodes which are copies/replicas of the primary name node. 

A file system is composed of directories and files where a directory contains zero or more directories and zero or more files.  Internal namespace objects called inodes store the attributes of files and directories and the metadata for them. For example, the attributes of a file or directory include the name, size, last modification time, and owner. The metadata for a file is the list or index of storage blocks and their offsets in the file address space. For a directory, the metadata includes the list of files and subdirectories included in the directory.

In DCFS, the implementation of the namespace also includes inodes for link containers in addition to directories. The link containers are internal objects in DCFS, and thus are not visible through the public API of DCFS. Through the public API, DCFS provides a typical file system namespace. 

Link containers are used to offload the file metadata from name nodes to data nodes. They are implemented as leaf directories in local file systems in data nodes and the files in these directories are link files pointing to the data containers holding the data files. The link files are similar to symbolic links in file systems. The link file and the corresponding data file may not be stored in the same local file system. These link directories include only link files and thus no sub directories. That is why they are leaf directories in the namespace of their local file systems. A DCFS name node maintains only inode for a link container. The content of the link container, that is the list of link files, is maintained by the local file systems in the corresponding link directory.

As the files in the DCFS namespace map to the files in local file systems, name nodes store inodes only for directories and link containers. Thus, a directory inode in a name node points to the inodes for link containers and sub directories. This minimizes the number of the inodes to be stored in name nodes and results in a scalable namespace. For example, by keeping N file links in a link directory, the in-memory footprint for inodes stored in name nodes is reduced by approximately N times. N can be in thousands.

DCFS can be used as a physical or virtual distributed file system. We will first explain DCFS as a physical distributed file system, then we will explain how it can be used as a virtual distributed file system.

## DCFS : A Physical Distributed File System

When DCFS is used as a physical file system, it is responsible to store files too in addition to the file system namespace. Internally data, that is, data files, are stored in data containers that are also directories in local file systems. Files in DCFS map directly to data files in local file systems. Implementing DCFS files as local file system files further reduces the metadata stored in name nodes. This means in addition to the dramatic reduction in the number of inodes stored in name nodes, name nodes do not keep track of file blocks and this further reduces the metadata maintained by name nodes.

One consequence of mapping files in DCFS to the files in local file systems is that a given file will be contained in a local file system and the size of the file will be limited with the available space in that local file system. This is intentional as DCFS is designed for use cases where file sizes will not be huge. This is the typical usage pattern in distributed systems where a large object is sharded and these shards are distributed over the application compute nodes. For example, DCFS will be a good choice as a file system for distributed key-value stores where files are split when they reach a predefined size, their I/O load exceeds a threshold, or some other condition happens. This split operation is necessary to scale key-value stores horizontally. An example of such a key-value store is HBase.

In DCFS, redundancy is built into containers by replicating the containers over data nodes such that each data node includes a single copy of a given container. Link containers are created when files are created in DCFS or when a link container becomes full as it is desired to limit the number of items in a directory in a local file system for performance and scalability reasons.

Data containers are created when needed as part of file creation in DCFS. Although the links in a link container are for the files of the same directory of the DCFS namespace, the DCFS files corresponding to these links are uniformly distributed over data containers in local file systems. This decoupling allows renaming a file without moving the file data.

Each container is uniquely identified by an ID called container ID. As mentioned above, there are two types of containers, link and data containers. DCFS keeps track of containers, location information for container replicas, and statistics for them such as their size and the number of files in them.

When a file is created, DCFS adds a link file and data file to a link container and a data container, respectively. The link container for the file is determined by the location of the file in the namespace (that is, the file path name). An existing link container for the file path will be used if the container is not full. Otherwise, a new link container will be created. For the data file, an available data container will be used. If there is no such available container then a new data container will be created. 

When a data node fails, DCFS starts a recovery process to recreate the link and data containers of this data node. To do that, it also maintains a reverse mapping from data nodes to the set of container Identifiers. If a copy of a container is not accessible, DCFS is responsible for creating another copy of the container in another host and then updating replica information of the container.

Each link container is associated with a bloom filter and these bloom filters are maintained as part of the metadata for link containers in name nodes. When a directory has a large number of files in the DSFN namespace, multiple link containers are needed. To traverse the namespace efficiently, bloom filters are used to map a file name to one or more link containers. If the namespace traversal for a file name is resolved to more than one container then, DCFS will issue lookups on the corresponding local file system directories to find the link file corresponding to the path name.

As mentioned before, a link container maps to a local file system directory and a link file maps to a file in that local directory. The content of a link file is the metadata maintained by DCFS for the corresponding file in the DCFS namespace. This metadata includes the data container ID of the data file.

The following figure illustrates how a file system namespace can be implemented in DCFS where the link directories and data files may have multiple replicas but only one replica is shown for simplicity. The location information for link directory replicas are stored in name nodes as mentioned before, however the location information for data file replicas are stored in link files.

Opening a file requires traversing the namespace stored in a name node to find the location of the file using the path name of the file. This traversal is done to find the identifier of the link container. Using the container table, the identifier is mapped to the location information. The next step is to retrieve the link file from the link container which is a directory in a local file system in a data node. This means that a remote file read operation is executed to retrieve the link file. In rare cases, due to false positives in bloom filters maintained for each link container, more than one parallel remote file read operation may be required to retrieve a link file. This is a rare case as the maximum number of link files stored in a link directory will be known a priori and the bloom filter size can be set in a way that the probability of false positives can be very low. For example, storing 8 bits per link file in a bloom filter will reduce the probability of false positives to around 0.02.  

A rename operation of a file does not require copying file data or recreating the file. After the link file is located in a link container, the link file is moved to the target link container. In this case, a flag to indicate if the link file is in move is set and the new path name for the link is stored in the link file. Then the new link is created in the target container. Finally, the old link file is removed. This means a rename operation requires reading the existing link file for the file to be renamed, updating it, creating a new link file, updating it, and finally removing the old link file. 

An application of DCFS interacts with a DCFS cluster using a client software component of DCFS which usually runs on the application server. The following figure illustrates the type interactions among an application server, name nodes, and data nodes. 

DCFS client interacts with the primary name node for namespace operations such as creating, deleting, and renaming a file or directory, opening a file, and listing the contents of a directory.  With creating or opening a file, a client retrieves information about the data nodes, link and data containers for the file. The client uses this information to read the content of the file, write to the file to update the content of the file, and/or update the attributes of a file. The operations on a file are done on the data nodes storing a replica of the data container for this file and thus the client interacts with these data nodes directly. 

The metadata information for a DCFS file is stored in the corresponding link file. Some file operations such as synchronizing a file (flushing in-memory content of the file to physical storage) and closing a file require updating the metadata including the last modification time and size of the file. These implicit metadata operations as well as explicit operations such as renaming a file and updating file attributes are done on the data nodes storing a replica of the link container for this file. 

## DCFS : A Virtual Distributed File System

DCFS can be used as a virtual file system on top of one or more possibly distributed file systems whose namespace is mounted to the DCFS namespace. In this type of deployment, the store provided by the local file systems in DCFS can become a tier and some of the files are stored in this tier. This tier can be used as a caching tier where files can be cached partially. Instead of caching an entire file, DCFS can cache a subset of the blocks of a file. The blocks or files that are not stored in DCFS can be retrieved from their physical file systems that are mounted within the DCFS namespace.

When the files are stored in mounted file systems, the link file points to these file systems. When the files are stored partially in the local file systems, in addition to the physical file system information, the information for the list of blocks that are cached are stored in the link files. 

