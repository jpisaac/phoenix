# Paginated Queries with Phoenix RVC Comparisons at Salesforce

As of October 2018, QueryMore and Feeds rely on non-standard Row Value Constructor, RVC, SQL behavior to paginate through their data.  The QueryMore team found some issues with RVC paginated behavior \[5\] which prompted investigation into RVC comparison pagination.  Found that there was an issue supporting SQL standard behavior and using RVC comparisons for pagination.  In addition, discussions with the Feeds team caused [https://issues.apache.org/jira/browse/PHOENIX-3383](https://issues.apache.org/jira/browse/PHOENIX-3383) to be written whose behavior partially corrected Phoenix RVC comparisons to behave more like standard SQL.  In order to mitigate risk the PHOENIX/HBase team is not shiping PHOENIX-3383 to salesforce forks the Salesforce use cases are properly handled.  In addition, tests were performed to better understand the risks currently in the 4.13-0.98 light fork.

# Usecases

The two primary users of Row Value Constructor, RVC, comparisons for pagination in Salesforce is to support the BigObject QueryMore cases \[1\]\[2\]\[7\] and for feed composition and score retrieval \[3\]\[4\].  

# Tests:

Recreated examples from QueryMore and Feeds as well as variety of queries and exhaustive queries were tested.  
Test Results spreadsheet (Selection of ran test cases): [https://docs.google.com/spreadsheets/d/1wb\_LvC0IpBTAPXK1E\_sdolL-iRP6MWSBT14\_8w8xm0o/edit?usp=sharing](https://docs.google.com/spreadsheets/d/1wb_LvC0IpBTAPXK1E_sdolL-iRP6MWSBT14_8w8xm0o/edit?usp=sharing)

# Summary

The main QueryMore and Feeds test cases pass providing correct behavior for pagination.    
2 main failures were found 1 of which is known and 1 which is not known   
  using RVC greater than comparisons for pagination.

1. The known issue of a non-fully qualified RVC even given a fully qualified key fails to push the key into the scan.  This error has a work around of over qualifying the key in the RVC expression. This error impacts performance only.  
2. A Table or Index with a leading key being DESC, RVC on the entire key.  This produces a scan with incorrect pagination as the scan pushes the RVC to the stop key rather than the start key.  This has no easy work around and will produce wrong results.

# 

# Part 2: Pagination with non-PK 

4.13

4.14  
0: jdbc:phoenix:localhost:58095\> SELECT \* FROM T000001 WHERE (K1,K2,D1)\>('b','b','9') ORDER BY D1 DESC LIMIT 4 ;  
\+-----+-----+-----+-----+  
| K1  | K2  | D1  | D2  |  
\+-----+-----+-----+-----+  
| b   | c   | 1   | 2   |  
| c   | b   | 1   | 2   |  
| c   | a   | 1   | 2   |  
\+-----+-----+-----+-----+  
3 rows selected (0.045 seconds)  
0: jdbc:phoenix:localhost:58095\> SELECT \* FROM T000001 WHERE (K1,K2,D1)\>('b','b','0') ORDER BY D1 DESC LIMIT 4 ;  
\+-----+-----+-----+-----+  
| K1  | K2  | D1  | D2  |  
\+-----+-----+-----+-----+  
| b   | b   | 1   | 2   |  
| c   | b   | 1   | 2   |  
| c   | a   | 1   | 2   |  
| b   | c   | 1   | 2   |  
\+-----+-----+-----+-----+

# Appendix

1. Splunk Query for QueryMore [https://splunk-web.crz.salesforce.com/en-US/app/search/search?sid=1541163214.134984\_FC4830B4-173C-4A24-A50A-075E5E2DFF2C](https://splunk-web.crz.salesforce.com/en-US/app/search/search?sid=1541163214.134984_FC4830B4-173C-4A24-A50A-075E5E2DFF2C)   
2. Example QueryMore  
   SELECT /\*+ NO\_CACHE \*/ C00NB0000004SYACMAG, C00NB0000004SYAAMAG, C00NB0000004SYAZMAW, C00NB0000004SYABMAG, CREATED\_DATE, C00NB0000004SYAYMAW,KEY\_PREFIX,C00NB0000004SYAYMAW,C00NB0000004SYAAMAG,C00NB0000004SYAZMAW FROM CUSTOM\_ENTITY."z00" WHERE (KEY\_PREFIX,C00NB0000004SYAYMAW,C00NB0000004SYAAMAG,C00NB0000004SYAZMAW) \> (?,?,?,?) LIMIT 2000  
3. Feeds read/write use case [https://codesearch.data.sfdc.net/source/xref/app\_main\_core/app/main/core/chatter/java/src/core/chatter/feeds/composition/FeedCompositionPhoenixQuery.java\#203](https://codesearch.data.sfdc.net/source/xref/app_main_core/app/main/core/chatter/java/src/core/chatter/feeds/composition/FeedCompositionPhoenixQuery.java#203) note use of different fields depending on the sort order parameter  
4. Feeds Top posts use case [https://codesearch.data.sfdc.net/source/xref/app\_main\_core/app/main/core/sbi/java/src/recommend/topentity/EntityScoreQueryServiceImpl.java\#81](https://codesearch.data.sfdc.net/source/xref/app_main_core/app/main/core/sbi/java/src/recommend/topentity/EntityScoreQueryServiceImpl.java#81) and [https://codesearch.data.sfdc.net/source/xref/app\_main\_core/app/main/core/sbi/java/src/recommend/topentity/EntityScoringServiceImpl.java\#145](https://codesearch.data.sfdc.net/source/xref/app_main_core/app/main/core/sbi/java/src/recommend/topentity/EntityScoringServiceImpl.java#145)  
5. QueryMore currently failing use case [https://gus.lightning.force.com/lightning/r/ADM\_Work\_\_c/a07B0000004p9ojIAA/view](https://gus.lightning.force.com/lightning/r/ADM_Work__c/a07B0000004p9ojIAA/view).  
6. Feeds tracking work item for PHOENIX-3383 changes. [https://gus.lightning.force.com/lightning/r/ADM\_Work\_\_c/a07B0000002rL03IAE/view](https://gus.lightning.force.com/lightning/r/ADM_Work__c/a07B0000002rL03IAE/view)  
7. QueryMore end user documentation [https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/sforce\_api\_calls\_querymore.htm\#topic-title](https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/sforce_api_calls_querymore.htm#topic-title)