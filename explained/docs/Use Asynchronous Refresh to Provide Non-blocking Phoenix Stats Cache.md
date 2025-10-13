# Use Asynchronous Refresh to Provide Non-blocking Phoenix Stats Cache

Started by [Bin Shi](https://salesforce.quip.com/EZXAEALwTBE) to improve phoenix Stats Cache

## Problem

Below is the high level picture of Phoenix Stats Cache which is based on Google Guava cache.
[Image: image.png]The current Phoenix Stats Cache uses TTL based eviction policy. A cached entry will expire after a given amount of time (900s by default) passed since the entry's been created. This will lead to cache miss when Compiler/Optimizer fetches stats from cache at the next time. As you can see from the above graph, fetching stats from the cache is a blocking operation — when there is cache miss, it has a round trip over the wire to scan the SYSTEM.STATS Table and to get the latest stats info, rebuild the cache and finally return the stats to the Compiler/Optimizer. Whenever there is a cache miss, this blocking call causes significant performance penalty — you can see periodic spikes from the following two graphs (Data Source: [_Performance test - Phoenix Client Cache Update_](https://salesforce.quip.com/iqcqAxSvQr3a) )

**GPW: 10 MB, Cache size: 29MB  Interval - 2 Mins ([Splunk Query](https://ice-splunksrch1-0-prd.eng.sfdc.net/en-US/app/search/search?q=search%20host%3D%22mist14*%22%20logRecordType%3Dpysql%20starttime%3D03%2F23%2F2018%3A01%3A28%3A00%20endtime%3D03%2F23%2F2018%3A02%3A00%3A00%20%20%7C%20rex%20field%3DtheRest%20%22.*FROM(%3F%3CWhereclause%3E.*)%60%60SUCCESS%60%60qt%3D.*%2C.*%2CsoqlTimeMs%3D(%3F%3CsoqlTime_MS%3E%5Cd%2B)%60SOQL_QUERY%60%22%20%7C%20timechart%20span%3D5s%20%20avg(soqlTime_MS)%20by%20Whereclause&earliest=-15m&latest=now&display.page.search.mode=verbose&display.page.search.tab=visualizations&display.general.type=visualizations&sid=1521826716.20177_6BBA2E98-6A98-4B1F-9C47-EFA26A35485F))**

[Image: 10MBGPW_2MinUpdate.png]
**GPW: 100 MB, Cache size: 2.9 MB Update Interval - 2 Mins ([Splunk Query](https://ice-splunksrch1-0-prd.eng.sfdc.net/en-US/app/search/search?q=search%20host%3D%22mist14*%22%20logRecordType%3Dpysql%20starttime%3D03%2F23%2F2018%3A19%3A26%3A00%20endtime%3D03%2F23%2F2018%3A20%3A01%3A00%20%20%7C%20rex%20field%3DtheRest%20%22.*FROM(%3F%3CWhereclause%3E.*)%60%60SUCCESS%60%60qt%3D.*%2C.*%2CsoqlTimeMs%3D(%3F%3CsoqlTime_MS%3E%5Cd%2B)%60SOQL_QUERY%60%22%20%7C%20timechart%20span%3D5s%20%20avg(soqlTime_MS)%20by%20Whereclause&earliest=-15m&latest=now&display.page.search.mode=verbose&display.page.search.tab=visualizations&display.general.type=visualizations&display.prefs.fieldFilter=enti&sid=1521835098.5836_605594B1-B8A7-411D-8F19-083BE776FCC6))**

[Image: 100MBGPW_2MinUpdate.png]
## Solution

### High Level Design

We can use Google Guava Cache refresh mechanism in asynchronous mode to fix the above issue — the cache periodically reload() the stats in which an asynchronous task is scheduled to the fetch the stats and rebuild the cache. During the refreshing, the old value (if any) is still returned while the key is being refreshed, in contrast to eviction, which forces retrievals to wait until the value is loaded anew. If an exception is thrown while refreshing, the old value is kept, and the exception is logged and swallowed. The whole process can depicted by the following graph.
[Image: image.png]
### Detail Design

We need to implement the Google Guava CacheLoader and override the load() and reload() methods. The reload() provides the asynchronous behavior. When using builder patter to build the cache,  automatically timed refreshing is added to the cache by CacheBuilder.refreshAfterWrite(long, TimeUnit). Below is the code for main logic.

```

public GuidePostsCache(...) {

    ExecutorService executor = Executors.newFixedThreadPool(10);
    
    // refreshes will be done asynchronously.
    LoadingCache<GuidePostsKey, GuidePostsInfo> statsCache =
        CacheBuilder.newBuilder()
           .maximumSize(1000)
           .refreshAfterWrite(900000, TimeUnit.MILLISECONDS)
           .build(new StatsLoader(executor));
}

protected class StatsLoader extends CacheLoader<GuidePostsKey, GuidePostsInfo> {
     public StatsLoader(ExecutorService executor)
     {
        this.executor = executor;
     }
     
     public GuidePostsInfo load(GuidePostsKey key)
     {
        return getStatsFromStatsTable(key);
     }

     public ListenableFuture<GuidePostsInfo> reload(
            final GuidePostsKey key,
            GuidePostsInfo prevGuidepostInfo)
         {
           if (needsRefresh(GuidePostsKey) == false) {
             return Futures.immediateFuture(prevGuidepostInfo);
           } else {
             // schedule asynchronous task
             ListenableFutureTask<GuidePostsInfo> task =
                ListenableFutureTask.create(
                    new Callable<GuidePostsInfo>() {
                        public GuidePostsInfo call() {
                            return getStatsFromStatsTable(key);
                        }
                    }
                );
             executor.execute(task);
             return task;
           }
        }
     }
}
```

Guideposts Cache will create ExecutorService object and pass to StatsLoader.

## Open Questions

### How to provide Resource Quota on the size of stats per table?

This can be handled by getStatsFromStatsTable(). In the function, it firstly get stats from stats table, then calculate the cache entry size and compare to the resource quota get from table object by invoking queryServices.getTable(). If the cache entry size is larger than the resource quota, then the function will throw the exception.

Quota per Table

### How to support different refresh cycle for different table?

Now the duration passed to CacheBuilder.refreshAfterWrite() is the duration to check whether we need to reload a particular cache entry, which is denoted as T_Check. The refresh duration of a table is defined to be the integral multiple of T_Check. The StatsLoader.reload() is invoked for every period of T_Check in which needsRefresh() will be called to check whether we need to reload the stats for the given cache entry based on the refresh duration of the table.

Separate TTL for each table -  Work item

### How to support different Client Cache settings/BEhaviors for different table groups?

Suppose we want to support different client cache settings/behaviors for different table groups (a table group can be defined as the tables belonging to one customer)? We can create a Guava cache instance for each table group with different settings, and GuidepostCache wrap all the cache instances and expose to the outside as just one instance.


