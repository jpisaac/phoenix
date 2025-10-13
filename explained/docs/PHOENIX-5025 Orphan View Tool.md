# Orphan View Tool

A view without its base table is an orphan view. Since views are virtual tables and their data is stored in their base tables, they are useless when they become orphans. A base table can have child views, grandchild views and so on. Due to some reasons/bugs, when a base table or view was dropped, its views were not not properly cleaned up in the past. For example, the drop table code did not support cleaning up grandchild views. This has been recently fixed by [PHOENIX-4764](https://issues.apache.org/jira/browse/PHOENIX-4764). Although [PHOENIX-4764](https://issues.apache.org/jira/browse/PHOENIX-4764) prevents new orphan views due to table drop operations, it does not clean up existing orphan views. It is also believed that when the system catalog table was split due to a bug in the past, it also contributed to creating orphan views as Phoenix did not support splittable system catalog. Therefore, Phoenix needs a tool to clean up orphan views.

# Background

There are several table types in Phoenix, such as Table Types: SYSTEM, TABLE, VIEW, INDEX, etc. This tool is concerned about the tables of type VIEW. 

A given Phoenix table may have one or (child) views and these views may have their own views. Thus, a given table may have a tree of views rooted at this given table. The table at the root of this tree will be called a *base* table in this note. A view is a virtual table and its data is stored in its base table. A base table is a physical table, i.e., has its own HBase table. Every view in a given view tree has a back pointer (or link) to the root (i.e., the base table). This back pointer is of type PTable.LinkType.PHYSICAL\_TABLE. In addition, link PHYSICAL\_TABLE,  a link of type PTable.LinkType.CHILD\_TABLE is maintained from a base table or a view to its immediate child views. If the parent of a view is another view, a link of type PTable.LinkType.PARENT\_TABLE is maintained from the child to the parent as shown in the figure below.

A view becomes orphan if one or more of the following conditions are true:

1. Its base does not exist.  
2. Its physical link does not exist.  
3. It is not reachable from its base table thru the CHILD\_TABLE links.  
4. Any of its ancestors' (parent, grandparent, etc) views does not exist.  
5. Any of its ancestors views is not reachable through the PARENT\_TABLE links.  

A (PHYSICAL\_TABLE, PARENT\_TABLE, or CHILD\_TABLE) link will be referred to as an orphan link if the source and/or the destination object of the link does not exist in the Phoenix system tables.

Table and view records, and the links are maintained in the SYSTEM.CATALOG table except that the CHILD\_TABLE links are maintained in the SYSTEM.CHILD\_LINK table from Phoenix release 4.15 onward.  

The orphan view tool scans these tables to identify and/or clean up the orphan view records. This tool does not aim to repair referential integrity issues beyond cleaning up orphan views.

# Detail Design

The orphan view tool does not scan any user table or delete any HBase table. It identifies and/or removes the orhan view records from the system tables. Since these tables rearly split into multiple regions, the tool will be implemented in the simplest fashion as a centralized and single-threaded tool.  

The tool can identify the orphan views and remove them immediately in one execution, or first identify and log orphan views in a file and then remove them later in a separate execution based on user input. The latter option can be used if the user wants to verify that the views that are identified as orphan are really orphan views. 

## Identifying Orphan Views and Links

The tool forms the view trees (as illustrated above) in memory. The views that are identified as orphan are added to an HashMap structure called orphanViewSet. The base tables (i.e., roots of the view trees) are maintained in an HashMap structure called baseSet. An array of HashMap structures (called viewSetArray) is maintained such that child views are maintained in viewSetArray\[0\], grandchild views in viewSetArray\[1\], and so on. The tool constructs viewSetArray structures level by level. During this process,it identifies orphan views and links and adds them to orphanViewSet and orphanLinkSet, respectively. This process is explained below in detail.

### Step 1: Identify all the views that are not MAPPED and populate orphanViewSet

The tool starts with identifying all the views that are not MAPPED views using the following query.  
String viewQuery \= "SELECT " \+  
       TENANT\_ID \+ ", " \+  
       TABLE\_SCHEM \+ "," \+  
       TABLE\_NAME \+  
       " FROM " \+ SYSTEM\_CATALOG\_NAME \+  
       " WHERE "+ TABLE\_TYPE \+ " \= '" \+ PTableType.VIEW.getSerializedValue() \+   
 "' AND NOT " \+  
 VIEW\_TYPE \+ " \= " \+ PTable.ViewType.MAPPED.getSerializedValue();

A MAPPED view is a read-only view of an HBase table instead of a Phoenix table. These views are excluded as they never have a Phoenix base table. For every view identified by the above query, an in-memory object of type View is added to orphanViewSet.

### Step 2: Identify all the candidate base tables and add them to baseSet

The tables that can be a base table (i.e., that are not views) are identified using the following query and the corresponding Base objects are added to baseSet.

String candidateBaseTableQuery \= "SELECT " \+  
       TENANT\_ID \+ ", " \+  
       TABLE\_SCHEM \+ "||'.'||" \+  
       TABLE\_NAME \+ " AS BASE\_TABLE\_FULL\_NAME" \+  
       " FROM " \+ SYSTEM\_CATALOG\_NAME \+  
       " WHERE "+ " NOT " \+ TABLE\_TYPE \+ " \= '" \+  
 	 PTableType.VIEW.getSerializedValue() \+ "'";

### Step 3: Identify all the PHYSICAL\_TABLE links, check if they are orphan, and update the View objects of orphanViewSet

The PHYSICAL\_TABLE links are identified using the following query.

String physicalLinkQuery \= "SELECT " \+  
       TENANT\_ID \+ ", " \+  
       TABLE\_SCHEM \+ ", " \+  
       TABLE\_NAME \+ ", " \+  
       COLUMN\_NAME \+ " AS PHYSICAL\_TABLE\_TENANT\_ID, " \+  
       COLUMN\_FAMILY \+ " AS PHYSICAL\_TABLE\_FULL\_NAME " \+  
       " FROM " \+ SYSTEM\_CATALOG\_NAME \+  
       " WHERE "+ LINK\_TYPE \+ " \= " \+  
       PTable.LinkType.PHYSICAL\_TABLE.getSerializedValue();

For every PHYSICAL\_TABLE link from a view to its base table, the corresponding View object is updated with the link information if the object exists in orphanViewSet and the base table exists in baseSet. If not, the link is added to orphanLinkSet.

### Step 4: Identify all the PARENT\_TABLE links, check if they are orphan, and update the View objects of orphanViewSet

The PARENT\_TABLE links are identified using the following query.

String childParentLinkQuery \= "SELECT " \+  
       TENANT\_ID \+ ", " \+  
       TABLE\_SCHEM \+ ", " \+  
       TABLE\_NAME \+ ", " \+  
       COLUMN\_NAME \+ " AS PARENT\_TABLE\_TENANT\_ID, " \+  
       COLUMN\_FAMILY \+ " AS PARENT\_TABLE\_FULL\_NAME " \+  
       " FROM " \+ SYSTEM\_CATALOG\_NAME \+  
       " WHERE "+ LINK\_TYPE \+ " \= " \+  
       PTable.LinkType.PARENT\_TABLE.getSerializedValue();

For every PARENT\_TABLE link from a child view to its parent view , the corresponding child View object is updated with the link information if the parent and view object exist in orphanViewSet. If not, the link is added to orphanLinkSet.

### Step 5: Identify all the CHILD\_TABLE links, check if they are orphan, and update the View objects of orphanViewSet and the Base objects of baseSet

The CHILD\_TABLE links are identified using the following query:

String parentChildLinkQuery \= "SELECT " \+  
       TENANT\_ID \+ ", " \+  
       TABLE\_SCHEM \+ ", " \+  
       TABLE\_NAME \+ ", " \+  
       COLUMN\_NAME \+ " AS CHILD\_TABLE\_TENANT\_ID, " \+  
       COLUMN\_FAMILY \+ " AS CHILD\_TABLE\_FULL\_NAME " \+  
       " FROM " \+ SYSTEM\_CHILD\_LINK\_NAME \+  
       " WHERE "+ LINK\_TYPE \+ " \= " \+  
       PTable.LinkType.CHILD\_TABLE.getSerializedValue();

For every CHILD\_TABLE link from a parent view or base table to its child view , the corresponding object is updated with the link information if the object exists in orphanViewSet or baseSet. If the view or parent does not exist,  the link is added to orphanLinkSet.

### Step 6: Remove the base tables with no child view from baseSet

### Step 7: Remove the child views of the base tables (of baseSet) from orphanViewSet  and add them to viewSetArray\[0\] if these views have the correct PHYSICAL\_TABLE link

### Step 8: Remove the child views of viewSetArray\[N\] from orphanViewSet and add them to viewSetArray\[N+1\] if these views have the correct the PHYSICAL\_TABLE and PARENT\_TABLE link 

## Dropping Orphan Views and Links

After the process of identifying orphan views and links described above, the views that are listed in orphanViewSet are the orphan views and the links that are listed in orphanLinkSet are the orphan links. The views and links will be deleted based on user input as described in the following section. There are two types of internal delete operation for views: graceful and forceful. In the graceful delete operation, the views are deleted using the normal drop table operations. However, due to referential integrity issues or missing objects, the drop table command may not successfully delete these views. Therefore, the tool attempts to delete orphan views first gracefully and then attempts to delete them forcefully. The forceful delete will delete the records of orphan views from the system tables if they exist. The records of the orphan links are removed from the system tables if the clean option is specified as described below.   
   
Examples for input arguments for the orphan view tool (OrphanViewTool) are:

* \-c : Orphan views can be cleaned up by just passing the “-c” (or “--clean”) option. The tool logs the row keys for the orphan views to the console.  
* \-c \-op /tmp/ : In addition to the “-c” option, if the “-op” (or “--output\_path”) option is given to identify the directory of the files to be written, then the tool cleans up orphan views and orphan links and logs the row keys of these views and links to the files.   
*  \-i : The option “-i” (or “--identify”) identifies orphan views and links and prints the row keys of them on the console.  
* \-i \-op /tmp/ : In addition to the “-i” option, if the “-op” (or “--output\_path”) option is given to identify the directory of the files to written, then the tool identifies orphan views and links and logs the row keys of these views to the files.  
* \-c \-ip /tmp/ :  In addition to the “-c” option, if the “-ip” (or “--input\_path”) option is given to identify the directory of the files listing orphan views and links, then the tool cleans up orphan views and links whose row keys are listed in the files.  
* \-a \<age\> : The option “-a” (or “--age”) specifies the minimum age (in milliseconds) for the orphan views. The default age is 1 day (24\*60\*60\*1000 milliseconds).

The  files generated by the tool are as follows:

* OrphanView.txt: This file includes a separate line for each orphan view in the format of \<tenant Id\>**,**\<schema name\>**,**\<table name\>.  
* OrphanPhysicalTableLink.txt: This file includes a separate line for each orphan PHYSICAL\_TABLE link  in the format of \<view tenant Id\>**,**\<view schema name\>**,**\<view name\>**\--\>**\<base table tenant Id\>**,**\<base table schema name\>**,**\<base table name\>.  
* OrphanParentTableLink.txt: This file includes a separate line for each orphan PARENT\_TABLE link  in the format of \<view tenant Id\>**,**\<view schema name\>**,**\<view name\>**\--\>**\<parent view tenant Id\>**,**\< parent view schema name\>**,**\<parent view name\>.  
* OrphanChildTableLink.txt: This file includes a separate line for each orphan CHILD\_TABLE link  in the format of \<parent tenant Id\>**,**\<parent schema name\>**,**\<parent name\>**\--\>**\<child view tenant Id\>**,**\< child view schema name\>**,**\< child view name\>.

The tool takes a snapshot of the system tables before attempting to delete orphan child views and links.