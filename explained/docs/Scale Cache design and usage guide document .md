  
**Scale Cache design and usage guide document**  
*By Sameer Khan, Sanjaya Lai, Frank Leahy, Balaji Iyer, Chaitanya Vagvala*   
*(Community & B2B Commerce Cloud perf/scale & dev teams)*

[**Introduction**](#introduction)	**[3](#introduction)**

[**History & Motivation**](#history-&-motivation)	**[3](#history-&-motivation)**

[**Overall architecture & design**](#overall-architecture-&-design)	**[4](#overall-architecture-&-design)**

[Key design principles & features](#key-design-principles-&-features)	[4](#key-design-principles-&-features)

[Modules Overview](#modules-overview)	[6](#modules-overview)

[Key highlights](#key-highlights)	[6](#key-highlights)

[Design and Usage Guide](#design-and-usage-guide)	[7](#design-and-usage-guide)

[Cache SYNC Refresh](#cache-sync-refresh)	[9](#cache-sync-refresh)

[TTL only invalidation strategy design & on-boarding steps](#heading)	[9](#heading)

[On-boarding steps](#on-boarding-steps)	[9](#on-boarding-steps)

[Create a cache object wrapper, referred to as ‘target-type’](#create-a-cache-object-wrapper,-referred-to-as-‘target-type’)	[9](#create-a-cache-object-wrapper,-referred-to-as-‘target-type’)

[Design and configure cache key (CK)](#design-and-configure-cache-key-\(ck\))	[10](#design-and-configure-cache-key-\(ck\))

[Create a value provider](#create-a-value-provider)	[12](#create-a-value-provider)

[Whitelist target type(s) for deserialization](#whitelist-target-type\(s\)-for-deserialization)	[13](#whitelist-target-type\(s\)-for-deserialization)

[(Optional) Add target-type to L1 cache consumers whitelist](#\(optional\)-add-target-type-to-l1-cache-consumers-whitelist)	[14](#\(optional\)-add-target-type-to-l1-cache-consumers-whitelist)

[Use of Scale Cache getValue() APIs to consume cache service](#use-of-scale-cache-getvalue\(\)-apis-to-consume-cache-service)	[14](#use-of-scale-cache-getvalue\(\)-apis-to-consume-cache-service)

[Object State change based invalidation strategy](#object-state-change-based-invalidation-strategy)	[15](#object-state-change-based-invalidation-strategy)

[Invalidation strategy design](#invalidation-strategy-design)	[16](#invalidation-strategy-design)

[On-boarding steps](#on-boarding-steps-1)	[20](#on-boarding-steps-1)

[Create a cache object wrapper, referred to as ‘target-type’](#create-a-cache-object-wrapper,-referred-to-as-‘target-type’-1)	[20](#create-a-cache-object-wrapper,-referred-to-as-‘target-type’-1)

[Design & configure cache key (CK)](#design-&-configure-cache-key-\(ck\))	[21](#design-&-configure-cache-key-\(ck\))

[Create a value provider](#create-a-value-provider-1)	[21](#create-a-value-provider-1)

[Whitelist target type(s) for deserialization](#whitelist-target-type\(s\)-for-deserialization-1)	[21](#whitelist-target-type\(s\)-for-deserialization-1)

[(Optional) Add target-type to L1 cache consumers whitelist](#\(optional\)-add-target-type-to-l1-cache-consumers-whitelist-1)	[21](#\(optional\)-add-target-type-to-l1-cache-consumers-whitelist-1)

[Design & configure object version key (OVK)](#design-&-configure-object-version-key-\(ovk\))	[21](#design-&-configure-object-version-key-\(ovk\))

[Hook up OVK(s) to the corresponding write path](#hook-up-ovk\(s\)-to-the-corresponding-write-path)	[22](#hook-up-ovk\(s\)-to-the-corresponding-write-path)

[Use of Scale Cache getValue() APIs to consume cache service](#use-of-scale-cache-getvalue\(\)-apis-to-consume-cache-service-1)	[27](#use-of-scale-cache-getvalue\(\)-apis-to-consume-cache-service-1)

[Cache ASYNC Refresh](#cache-async-refresh)	[30](#cache-async-refresh)

[Design](#design)	[31](#design)

[On-boarding steps](#on-boarding-steps-2)	[31](#on-boarding-steps-2)

[Create a cache object wrapper, referred to as ‘target-type’](#create-a-cache-object-wrapper,-referred-to-as-‘target-type’-2)	[31](#create-a-cache-object-wrapper,-referred-to-as-‘target-type’-2)

[Design & configure cache key (CK)](#design-&-configure-cache-key-\(ck\)-1)	[32](#design-&-configure-cache-key-\(ck\)-1)

[Whitelist target type(s) for deserialization](#whitelist-target-type\(s\)-for-deserialization-2)	[32](#whitelist-target-type\(s\)-for-deserialization-2)

[(Optional) Add target-type to L1 cache consumers whitelist](#\(optional\)-add-target-type-to-l1-cache-consumers-whitelist-2)	[32](#\(optional\)-add-target-type-to-l1-cache-consumers-whitelist-2)

[Create a Scale Cache ACTION TYPE ENUM](#create-a-scale-cache-action-type-enum)	[32](#create-a-scale-cache-action-type-enum)

[Map ACTION ENUM to the corresponding message handler](#map-action-enum-to-the-corresponding-message-handler)	[32](#map-action-enum-to-the-corresponding-message-handler)

[Add a bean for the message handler](#add-a-bean-for-the-message-handler)	[33](#add-a-bean-for-the-message-handler)

[Create a message handler](#create-a-message-handler)	[33](#create-a-message-handler)

[Use of Scale Cache getValue() APIs to consume cache service](#use-of-scale-cache-getvalue\(\)-apis-to-consume-cache-service-2)	[35](#use-of-scale-cache-getvalue\(\)-apis-to-consume-cache-service-2)

[Counter Tracker Service](#counter-tracker-service)	[37](#counter-tracker-service)

[Design](#design-1)	[37](#design-1)

[On-boarding Steps](#on-boarding-steps-3)	[39](#on-boarding-steps-3)

[Create a counter wrapper, referred to as ‘target-type’](#create-a-counter-wrapper,-referred-to-as-‘target-type’)	[39](#create-a-counter-wrapper,-referred-to-as-‘target-type’)

[Design & configure counter cache key (CCK)](#design-&-configure-counter-cache-key-\(cck\))	[39](#design-&-configure-counter-cache-key-\(cck\))

[Create a counter value provider](#create-a-counter-value-provider)	[39](#create-a-counter-value-provider)

[Hook up counter \#incr/decr operations to the corresponding write path](#hook-up-counter-#incr/decr-operations-to-the-corresponding-write-path)	[40](#hook-up-counter-#incr/decr-operations-to-the-corresponding-write-path)

[Method\#1: Hooking into scale cache transaction observer](#method#1:-hooking-into-scale-cache-transaction-observer)	[40](#method#1:-hooking-into-scale-cache-transaction-observer)

[Method\#2: Directly utilizing scale cache \#incr/\#decr API’s via manual injection in the write path(s)](#method#2:-directly-utilizing-scale-cache-#incr/#decr-api’s-via-manual-injection-in-the-write-path\(s\))	[43](#method#2:-directly-utilizing-scale-cache-#incr/#decr-api’s-via-manual-injection-in-the-write-path\(s\))

[Use of Scale Cache getCounterValue() APIs to consume counter-tracker service](#use-of-scale-cache-getcountervalue\(\)-apis-to-consume-counter-tracker-service)	[43](#use-of-scale-cache-getcountervalue\(\)-apis-to-consume-counter-tracker-service)

[**Loglines**](#loglines)	**[44](#loglines)**

[LogRecordType](#logrecordtype)	[44](#logrecordtype)

[\>csgcs](#\>csgcs)	[44](#\>csgcs)

[\>scgen](#\>scgen)	[45](#\>scgen)

[\>scinv](#\>scinv)	[46](#\>scinv)

[**Key Features Release timelines**](#key-features-release-timelines)	**[46](#key-features-release-timelines)**

[**MISC**](#misc)	**[47](#misc)**

# **Introduction** {#introduction}

Scale Cache is an intelligent, feature rich, highly scalable cache abstraction, layered on top of (Redis based) CaaS service. It provides a comprehensive caching solution platform, for high throughput B2B and B2C read transactions in UI and Core tiers. This caching abstraction embeds design principles geared towards backend scalability, primarily the db and network.  
Scale Cache is built and maintained by [Community & B2B Commerce Performance Engineering team](https://gus.my.salesforce.com/_ui/core/chatter/groups/GroupProfilePage?g=0F9B000000006mT), whereas the CaaS layer is offered and maintained by the [sfdc CaaS team](https://gus.my.salesforce.com/_ui/core/chatter/groups/GroupProfilePage?g=0F9B00000002e0O). Scale Cache offers a number of unique features and characteristics over other cache abstractions (example: Distributed Cache, Direct memcached and/or CaaS access, UI SDK CacheUtil etc) available within salesforce, that make it a preferred platform of choice for caching needs across a wide array of use cases. 

Even though this service is developed and lo maintained by the community performance engineering team, it has evolved over time with strong design and code review contributions by various architects within community dev team, along with product management support.  

# **History & Motivation** {#history-&-motivation}

Scale Cache came into existence due to the pressing scale needs of Community Cloud’s B2C Communities offering. We regularly found ourselves struggling to support high volume potential deals (use cases involving anything more than \~500 page views per min), due to oracle/db overhead within the context of a single community/org. Since the community components lived within the ui tier, we examined existing cache abstractions that we can leverage either as-is or something we can incrementally enhance over time. The closest we got was UI SDK CacheUtil as a potential base framework to apply the enhancements (outlined in this document) to. Our choices were to either build these directly on top of that implementation and develop as a common code base OR fork that abstraction and evolve our version separately and independently based on our very own needs. After thorough discussions with the team owning CacheUtil, we mutually agreed on the latter option. That setup & arrangement have worked fairly well over time and scale cache has evolved into a strong feature set rich and scalable caching abstraction, that can be used in both tiers within salesforce, due to this offering being a shared module/service. 

# **Overall architecture & design** {#overall-architecture-&-design}

## **Key design principles & features** {#key-design-principles-&-features}

Since Scale Cache need came out of supporting high (read) throughput scalability use cases, its built upon the following design principles \- 

* Zero reliance on db for ANY of its supported invalidation strategies management \- for example: scale cache supports cached object db state change based invalidations, via object versioning scheme, without needing to maintain versions in the db. Instead it uses CaaS itself for version management   
* Minimize \# of pod-wide parallel app threads to just a single thread that loads a given cache object from db (or other primary data-source), upon cache expiration. This prevents cache stampede scenario where a large number of threads all fallback on db (upon a cache miss) to build the same cache, during heavy arrival rate time frames  
* Minimize \# of pod wide parallel app threads waiting for a given expired cache object reload (from db), by employing  
*  ‘extended TTL strategy’ where if a given object has expired it’s TTL but is still within a “grace period” (we call it extended TTL) AND is. As not invalidated (where applicable \- that is, if the cache object is using TTL \+ state change based invalidation strategy), its OK to continue to serve it back to the consumers, while a single thread reloads the object from db   
* Minimize pod-wide infrastructure network overhead and CaaS traffic, by implementing a L1 (app server) cache, that works in-sync with L2 (CaaS). This is important to have, given the arrival rates we have geared this solution towards AND non-trivial size of some of these cache objects  
* Have a library of (scalable) cache invalidation and cache rebuild strategies to cater the need of a wide array of use cases.   
  * The supported invalidation strategies include:   
    * TTL only (configurable value),   
    * TTL \+ State change based invalidation   
  * The cache build/rebuild (upon a cache miss) strategies include:   
    * SYNC refresh   
      * Involves building the cache object in the foreground (request) thread, with N \- 1 threads either:  
        * Wait for sync refresh op, where N is the total \# of concurrent threads performing a cache.get() for the object, while a single thread builds the object, OR  
        * Return (TTL expired) data IF it’s within extended TTL (“grace period”)  
    * ASYNC refresh   
      * Involves building the cache object in a background thread (via MQ in core), with all N request app threads either:  
        * Return stale data IF within extended TTL (“grace period”) OR   
        * Return null   
        * And in both of the above cases, a single thread en-queues cache refresh request in core via MQ \- hence called ‘async refresh’  
* No (hard) affinity to either tier (ui or core). That is, it is a shared module so that it can be leveraged by scale use cases within each of those tiers  
* Offers a counter tracker service, where a consumer can use scale cache to track and maintain counter(s) in L2 (CaaS) cache, for use cases where the counts would otherwise be either: a) expensive to compute-and-retrieve from primary data-source, b) are transient in-nature and therefore not backed by a persistent storage, c) are an input to either some computed metric OR act as a trigger for an action (ex: reward users with badges upon exceeding certain thresholds of logins per unit time, etc)  
  * Some example use cases falling under ‘expensive to compute-and-retrieve’ category would be:   
    * User counts against large volume of (existing) users, for license limit enforcement purpose \- example: high volume portal users are supported in the range of tens of millions and the count query progressively gets expensive as the scale increases  
    * Entity records counts, for enforcing a cap on high water mark due to scale reasons \- example: Account Relationship records count are subject to a max limit (in millions), within the context of an org, for scale reasons. The existing count is checked against this max limit anytime a record (or a set of records, via bulk inserts) are provisioned in an org  
  * Other example(s) under different buckets:  
    * Maintaining user logins counts per given interval, to award badges upon meeting or exceeding certain frequency of logins

## **Modules Overview** {#modules-overview}

Fig\#1

### **Key highlights** {#key-highlights}

* By virtue of being a shared module, scale cache as an offering is available in ui and core tiers  
* Its state change based invalidation strategy can also be leveraged in either tier, but invalidations can only happen from within core. In other words, a consumer can leverage caching within ui tier with state change based invalidation strategy but will have to invalidate the cache (on the write path) from within core. Scale Cache provides two primary mechanisms for invalidations (in core): 1\) via its transaction observer , 2\) via its invalidation api, offered through core-scalecache-services-api module. More on these later in the doc  
* Async refresh service can also be leveraged in either tier, but the cache is always build from within core, via sfdc MQ service. The async (cache) refresh request is en-queued in MQ (from the requesting tier) via rest API  
* Consumers are expected to only import public modules

## **Design and Usage Guide** {#design-and-usage-guide}

## 

Fig\#2. Scale Cache caching service flow

The cache loader employs a distributed lock strategy that involves coordinating cache load (from db) among requesting app threads, via a distributed (soft) lock key placement in CaaS. All but one contending threads (across POD) attempting to fetch the given cache object spin-wait while load (from a single thread) is in process, followed by fetching from L2 level cache (CaaS). This prevents the cache stampede scenario where a large number of threads all fallback on db to build the same cache, during heavy arrival rate time frames. 

The invalidations are only supported from within core with the reason being that Scale Cache invalidation service relies on MQ for re-tries incase of invalidation enqueue request failure in the sync (foreground) app thread. 

| Object Refresh op Type | TTL Only Invalidation strategy | TTL \+ State change based invalidation strategy |
| :---- | :---- | :---- |
| SYNC  | Supported | Supported |
| ASYNC | Supported | Not Supported |

Table\#1. Feature matrix

## 

Fig\#3. Scale Cache counter-tracker service flow

### **Cache SYNC Refresh** {#cache-sync-refresh}

Fig\#4. SYNC Refresh

####  {#heading}

#### **TTL only invalidation strategy design & on-boarding steps**

This invalidation strategy involves attaching a fixed (configurable) TTL to a given cache entry. The cache object no longer remains in cache post TTL period. However, Scale cache has this feature where, in order to minimize thread wait times (on cache reloads), we return the (expired) cached value IF its within a certain "grace period" called extended TTL (extended ttl \= ttl \+ .10% ttl, with min,max bounds) AND if the object is NOT explicitly invalidated (note: this is only applicable for entries employing cache state change based invalidation strategy and not TTL only scheme). 

##### **On-boarding steps** {#on-boarding-steps}

###### **Create a cache object wrapper, referred to as ‘target-type’**   {#create-a-cache-object-wrapper,-referred-to-as-‘target-type’}

The wrapper object encapsulates cacheable data. The cache obj wrapper along with all its children fields in the entire object graph, need to be serializable. Example (Reference: ReputationLeaderBoard.java, SimilarArticles.java, ManagedContentNodeType.java etc):

/\*\*  
 \* Object to cache LeaderBoard results  
 \*  
 \* @author sameer.khan  
 \* @since 210  
 \*/  
public class ReputationLeaderBoard implements Serializable {

    private final List\<UserSummary\> leadersList;  
    private final Map\<String, UserSummary\> leaderBoardData;

   public ReputationLeaderBoard (final Map\<String, UserSummary\>leaderBoardData) {  
     this.leaderBoardData \= Collections.unmodifiableMap(leaderBoardData);  
     this.leadersList= Collections.unmodifiableList(new ArrayList\<UserSummary\>(leaderBoardData.values()));  
 }  
         
   public final Map\<String, UserSummary\> getLeaderBoardData() {  
       return this.leaderBoardData;  
   }

   public List\<UserSummary\> getLeaderBoardDataAsList() {  
       return leadersList;  
   }  
    
}

###### **Design and configure cache key (CK)** {#design-and-configure-cache-key-(ck)}

Construct cache key using the provided CacheKeyBuilder. A cache key is a composite of N number of (supported) fields, and it’s raw field values are constructed via overloaded append() methods in the cache key builder utility. 

The builder uses murmur3 128-bit hash function to compute hash over the supplied fields. The hash is used in the built cache key \- this transformation happens within scale cache and is thus abstracted from the consumer. 

As part of this transformation, the hash is pre-appended by: 

* a static marker tagging the key as belonging to scale cache,   
* organization id (which is a mandatory field in the cache key)   
* (optional) network id field.   
* In addition, its post appended by target-type class name associated with the cache key. The target type is the class name of cached object value wrapper and is supplied as an arg when creating CacheKeyBuilder object. 

The purpose of the static marker is to avoid possible key duplications across other cache keys outside of scale cache, using CaaS.  

*Since there is hashing involved in cache key formation \- given this is murmur3 128-bit hash, for any realistic possible number of entries within a \*single\* org (even multiplied by some large number X), the probability of collision is **practically zero**. See [this](http://preshing.com/20110504/hash-collision-probabilities/) for a good reference on hash collision probability.* 

Note that even though the CaaS limits the cache key size to 210 chars, because of the transformation of the aggregated input values to a hash, a consumer can add fields adding up (**in raw size**) to \> 210 chars. The transformed scale cache key \- ‘\<scalecache marker string\>/orgId/\<optional\>networkId/\<hash\>/\<target type\>’ will always result in \< 210 chars because of this hashing. The transformation happens within scale cache and thus abstracted from the cache consumer.  

Example cache key:  
*SCK/00Dxx0000001k04/0DBxx00000000Bs/a00793e22df01c1a8ecf4c90c1ac6ea8/ArticleVersionId/*  
Where,   
SCK \== static marker for scale cache  
00Dxx0000001k04 \== org id   
0DBxx00000000Bs \== network id  
A00793e22df01c1a8ecf4c90c1ac6ea8 \== (computed) hash value over the raw cache key content  
ArticleVersionId \== target type for this cache key value, which is the object wrapper encapsulating the cached data

Example (Ref: ReputationLeaderboardDataProviderController.java)

private CacheKeyBuilder\<ReputationLeaderBoard\> createKey(Integer numOfUsers, String displayObject, Boolean contextUserEnabled, String knowledgeLabel, Boolean excludeInternalUsers, String lang) {  
        CacheKeyBuilder\<ReputationLeaderBoard\> cacheKeyBuilder \= scaleCacheUtilWrapper.getCacheKeyBuilder(ReputationLeaderBoard.class, AppVersion.CURRENT, METHOD);

cacheKeyBuilder.appendOrgId(orgService.getId());        cacheKeyBuilder.appendNetworkId(networkUtil.getNetworkIdFromContext());  
        /\*  
         \* Cache key contains user type (Standard, Portal etc). Since, we can have more than one profile per user type,  
         \* storing the profile id in cache key.  
         \*/  
        cacheKeyBuilder.append(userService.getUserType())  
                .append(userService.getProfileId())  
                .append(String.valueOf(numOfUsers))  
                .append(String.valueOf(contextUserEnabled))  
                .append(knowledgeLabel)  
                .append(displayObject)  
                .append(lang)  
                .append(String.valueOf(excludeInternalUsers));

Note that a consumer does not need to invoke cacheKeyBuilder.build(...) since its auto invoked within scale cache platform. 

//NOT RECOMMENDED practice  
LOGGER.fine(String.format("ReputationLeaderBoard cache strategy \[isCacheable, cacheKey\] \= \[%s, %s\]", true, cacheKeyBuilder.build()));

Even though ReputationLeaderboardDataProviderController.java (used here as sample code) also contains the above logline, this type of logging is usually a waste of resources, not providing much value. Instead, the consumers are encouraged to just use scale cache OOTB loglines \- more on this under loglines section later 

###### **Create a value provider** {#create-a-value-provider}

Value Provider encapsulates the logic of building the (cacheable) object from its primary datasource (usually db), in the case of cold cache or a cache miss. Consumers are responsible for providing implementation of this, for their associated cache key(s).

Example (Reference: SimilarArticlesDataProviderController.java):  
//Other examples: ReputationLeaderboardDataProviderController.java, ManagedContentSetupCachingUtilImpl

private SimilarArticles doCompute(CommunityId communityId, String userId, ConnectRecommendationActionEnum action, String recType, ID articleId, Integer limit) throws ServiceException {  
        try {  
            List\<AbstractRecommendationRepresentation\> recCollection \= recommendations.getRecommendationsForUser(null,communityId, userId, action, recType, action, articleId, null, limit).getRecommendations();  
            return new SimilarArticles(recCollection);  
        } catch (ConnectInJavaException x) {  
            if(x.getStatusCode() \== HttpStatusCode.FORBIDDEN.value) {  
                DiscoveryInfoLogRecord.log\_dxlog(logger, orgService.getId(), communityId.getRawValue(), userId, "Related Articles call returned 403", "Context articleId: " \+ IdConverter.idTo15NoThrow(articleId.get18CharId()), null);  
                return new SimilarArticles(Collections.emptyList());  
            } else {  
                throw new ServiceException(x);  
            }  
        }  
    }

private ValueProvider\<SimilarArticles\> createValueProvider(CommunityId communityId, String userId, ConnectRecommendationActionEnum action, String recType, ID articleId, Integer limit, int ttl) {  
        return new AbstractTimeoutBasedValueProvider\<SimilarArticles\>(ttl) {  
            @Override  
            protected SimilarArticles computeValue() throws ScaleCacheServiceException {  
                SimilarArticles similarArticles=null;  
                try {  
                    similarArticles=doCompute(communityId, userId, action, recType, articleId, limit);  
                } catch (ServiceException se) {  
                    /\*if this service exception encapsulates a throwable, we need to use that as the cause for ScaleCacheServiceException\*/  
                    throw new ScaleCacheServiceException(se.getCause() \== null ? se.getMessage() : se.getCause().getMessage(), se.getCause() \== null ? se : se.getCause());  
                }  
                return similarArticles;  
            }  
        };  
    }

###### **Whitelist target type(s) for deserialization** {#whitelist-target-type(s)-for-deserialization}

Given that java serializer has security hole on deserialize, we use our own custom (secure) version of ObjectInputStream, for use with the global cache. Deserialization attempts are filtered through this version and the objects allowed are gated by whitelisting strategy.  

The cache consumer needs to whitelist cache object (defined in step\#1) in shared.scalecache.impl.services.GlobalCacheInputStream*.* Please note that the entire cache object graph needs to be whitelisted. A common best practice is to simply add cache object package prefix to Pattern.compile(...). If any of the member objects (within the cache object graph) needs whitelisting (meaning they are not already whitelisted) AND their package is different from the (parent) cache object, you will need to add those packages as well to Pattern.compile.   
Before whitelisting, please carefully examine GlobalCacheInputStream to see whether your target objects are already whitelisted or not. 

If your cache object (or any of its member fields) are not whitelisted, you can expect to see the following error upon deserialization \- 

logRecordType=scgen “Unauthorized deserialization attempt \<offending class\>” 

Example (Reference: ui.self.service.components.model.SimilarArticles):  
The package prefix (ui.self.service) is added to Pattern.compile(...) in GlobalCacheInputStream. Additionally, its has AbstractRecommendationRepresentation as its member field. Since it extends ConnectRepresentation, it does NOT require explicit whitelisting \- any class extending ConnectRepresentation is implicitly whitelisted \- see whitelistBaseClasses collection members in GlobalCacheInputStream. All member fields of AbstractRecommendationRepresentation are also implicitly whitelisted. 

###### **(Optional) Add target-type to L1 cache consumers whitelist** {#(optional)-add-target-type-to-l1-cache-consumers-whitelist}

Scale cache also offers L1 cache, which shares heap with rest of the app jvm. This arrangement enforces usage constraints and therefore L1 usage is gated by whitelist (see whiteListForL1Cache collection in ScaleCacheUtilGlobalCacheImpl). As stated, this is an optional cache, use it if your use case does NOT involve a large number of distinct keys per org, to avoid potential thrashing. Please consult with Sameer Khan and/or Community Cloud Performance Engineering team for any questions/consultation regarding whether use of L1 is feasible for your given use case.

######  **Use of Scale Cache getValue() APIs to consume cache service** {#use-of-scale-cache-getvalue()-apis-to-consume-cache-service}

All of the publicly consumable APIs are exposed via ScaleCacheUtilWrapper.java \- 

\#getValue(CacheKeyBuilder\<T\> cacheKeyBuilder, ValueProvider\<T\> valueProvider)

/\* Read-only api \- attempts to fetch the value from cache. If the value does not exist in the cache, returns null \*/  
\#getValueWithNoLoad(CacheKeyBuilder\<T\> cacheKeyBuilder)

Example\#1 (See ReputationLeaderboardDataProviderController.java):   
...  
/\*  
             \* Uses scale cache to fetch the component data. Reduces the db calls to fetch the actual data. Access  
             \* checks are performed for authenticated user.They are NOT cached. Therefore, db calls would be limited to  
             \* access checks.  
             \*/  
            final CacheKeyBuilder\<ReputationLeaderBoard\> cacheKeyBuilder \= createKey(numOfUsers, displayObject, contextUserEnabled, knowledgeLabel, excludeInternalUsers, lang);  
            final ValueProvider\<ReputationLeaderBoard\> valueProvider \= createValueProvider(numOfUsers, displayObject,  
                    contextUserEnabled, knowledgeLabel, excludeInternalUsers, cacheTTLSeconds);  
            /\*  
             \* ToDo: W-4413231: To build the first time scale cache, use a MAD/admin user to prefetch the leaderboard  
             \* information for top N users. The cache will have a comprehensive list and as we cache more than the  
             \* required amount of data, any subsequent requests for an authenticated user would mostly be served from  
             \* the cache. We will move this logic to core.  
             \*/  
            result \= scaleCacheUtilWrapper.getValue(cacheKeyBuilder, valueProvider);  
...

#### **Object State change based invalidation strategy**  {#object-state-change-based-invalidation-strategy}

This strategy involves explicit invalidation of a cache object upon mutation. Use-cases needing this are scenarios bound by requirement of non-staleness OR traffic patterns that require higher TTL (\>= 24 hrs) for yielding healthy cache hit ratios. An example of the latter category is community knowledge article component \- a community with articles (and article feed), with wide data access pattern \- that is, article reads spread across thousands of distinct articles over the course of a day. In this scenario, the per-article access rate would rather be low (few times per day) but in aggregate, the throughput could still be relatively high enough to warrant caching, from scale standpoint. Same is applicable (although to a relatively smaller degree) for topics feed (in a wide array of topics dataset). For wide data-access pattern based scenarios such as these, a low TTL (in the order of minutes) will yield very low cache hit rates. Extending the TTL is not desired due to staleness issues. In addition, there are other use cases where a piece of data is directly mutable by the context user and in that scenario, the cached entity needs to be invalidated with the write transaction commit. Therefore, this strategy includes large TTL (we currently use 24 hrs) paired with obj state change based invalidation, where upon cache object state mutation (in db), the object gets invalidated in the cache.

Note that all of the above use case, however, still need to model high read/write ratio traffic pattern, for them to be candidates for caching. 

Scale cache offers couple of mechanisms to setup cache invalidations (on the write path) \- 

* Hooking involved transaction(s) write paths into scale cache transaction observer  
* Using Scale cachev invalidation API to perform invalidations 

The transaction observer option is scale cache’s preferred choice for setting up invalidations, however consumers are free to use either of the two available options. 

##### **Invalidation strategy design** {#invalidation-strategy-design}

Scale cache uses CaaS itself for versioning scheme management. The underlying core concept of the invalidation mechanism is that each target type (where target-type \= to-be-cached data representation wrapper) can have 1..N associated write actions that may mutate the state of that representation. Each target-type has a cache key definition associated with it. Each distinct write action is modeled by Object Version Key (OVK) that tracks/maintains latest \[atomic\] row level \#version (in CaaS) against the associated write action. Therefore, implicitly, each cache key is associated with 1..N OVKs. The cache keys go through version check(s) within scale cache, thus always returning the latest \[logical\] object state. *Note: The version\# (tracked/maintained by OVK(s)) is also part of the scale cache metadata info associated with the given cached value.* 

OVK version\# is simply a type long, with initial value seeded via an algorithm, and (atomically) incremented anytime a matching action occurs. OVK is init’d upon either a read or write action (whichever comes first). The algorithm seeding the initial value (anytime OVK version\# is init’d) makes it virtually impossible for the supplied value to be one that the system has ever seen before (for the same cache key).

OVK is (by-design) de-coupled from CK to allow a flexible design which can accommodate scenarios where inferring the complete CK is not possible on one or all associated write paths. Let’s say if a given CK is a composite of \[Field\[A\], Field\[B\], Field\[C\]\], but only A & B are available on the write path. Another reason being that there could be multiple (distinct) set write actions (each with a different set of key fields) that invalidate a given cache object, without the possibility to infer CK (associated with the cached object) from ALL of the write actions. In addition, the relationship between CK and OVK is many-to-many. A single CK can be assoicated with multiple OVKs and vice versa.   
 

**Visual layout of Target-Type, Cache Key (CK) and Object Version Key (OVK) relationship**  
    
Fig\#5

**Invalidation flow example \-**   
CaaS entries at T0 for CacheKey1

* CacheKey1=\[org1, acc1\], CacheValue=SCValueWrapper\[CacheValue obj, Map\<OVK\[acc1\], Version\#\[ex: 675\]\>, \<Other metadata that SC maintains\>\]  
* OVK\[acc1\]=vers\#\[675\]

Update event occurs on acc1 at T1  
CaaS entries at T1

* OVK\[acc1\]=vers\#\[676\] //incremented via scale cache transaction observer  
* CacheKey1=\[org1, acc1\], CacheValue=SCValueWrapper\[CacheValue obj, Map\<OVK\[acc1\], Version\#\[675\]\>, \<other metadata that SC maintains\>\]  **//CacheKey1 in INVALIDATED STATE**  

Read occurs at T2

* SC performs version check and determines OVK\[acc1\], Version\#\[676\] \!= 675 that’s stored in SCValueWrapper attached with CacheKey1  
* Performs cache refresh  
* Updates the cache 

CaaS entries at T2 

* CacheKey1=\[org1, acc1\], CacheValue=SCValueWrapper\[CacheValue obj, Map\<OVK\[acc1\], Version\#\[676\]\>, \<Other metadata that SC maintains\>\]  
* OVK\[acc1\]=vers\#\[676\]

All subsequent reads will go through this versioning check and will result in a cache-hit upon version match

**Example\#1: Cache Knowledge Article Published/Online version id**  
   
Fig\#6. Knowledge Article example   
In the above example, the cache object (target type) encapsulates the published/online article-version-id for a given article id. The cache key definition (CK\[A\]) is associated with a single OVK (OVK\[A\[1\]\]) definition, responsible for tracking invalidations against the CUD ops on the associated article id, for KnowledgeArticleVersion entity type. This particular use cases employs Scale Cache invalidation API (hooked within KnowledgeBasePublishingService) to queue invalidation in scale cache, against a list of KnowledgeArticle actions. Please see [this](https://docs.google.com/document/d/1AhLODS5ZE0lOiQDjxeIJbwtgubosUT_L1h5qvZA7uOg/edit) document for more complete design and usage details (if interested).

**Example\#2: Cache ManagedContentTypeVersion POJO**   
   
Fig\#7

In the above example, the cache object (target-type) represents ManagedContentTypeVersion POJO for a given managed-content-type-version id. The POJO encapsulates information computed via join between ManagedContentTypeVersion and ManagedContentType entity types \- the two entities have a parent-child relationship. The cache key definition (CK\[A\]) is associated with two OVKs (OVK\[A\[1\]\] & OVK\[A\[2\]\]), to accommodate this relationship. OVK\[A\[1\]\] comprise of OrgId and TypeId whereas OVK\[A\[2\]\] has OrgId and TypeVersionId as its member sub-keys. OVK\[A\[1\]\] tracks invalidations against CUD on ManagedContentType entity whereas OVK\[A\[2\]\] tracks against CUD on ManagedContentTypeVersion entity. A change to any of the two entity types invalidates the cached POJO.   
This particular use case uses scale cache transaction observer for performing invalidations.   
Please see [this](https://docs.google.com/document/d/1-h02hU03B1pnauyKtDGbxXoK02B-131nsbAU4_cwDKo/edit) document for complete design and usage details.

**Example\#3: Cache AccountRelationshipAccountToInfo**  

Fig\#8

In the above example, the cache object (target type) represents AccountRelationshipAccountToInfo POJO for a given \[AccountTo,Type\] pair. The cache key definition (CK\[A\]) is associated with two OVKs (OVK\[A\[1\]\] & OVK\[A\[2\]\]). OVK\[A\[1\]\] comprise of \[AccountTo,Type\] whereas OVK\[A\[2\]\] has AccountTo as its member sub-keys. OVK\[A\[1\]\] tracks invalidations against CUD on ARJunction entity whereas OVK\[A\[2\]\] tracks bulk invalidations (invalidations across all keys with a given AccountTo).   
This particular use cases employs Scale Cache invalidation API to enqueue invalidation in scale cache. Please see the below classes for further design and usage details \- 

AccountRelationshipAccountToInfoScaleCacheService.java  
AccountRelationshipAccountToCacheManager.java

##### **On-boarding steps** {#on-boarding-steps-1}

###### **Create a cache object wrapper, referred to as ‘target-type’**  {#create-a-cache-object-wrapper,-referred-to-as-‘target-type’-1}

This is exactly the sameexactly same as what’s outlined in [this](#create-a-cache-object-wrapper,-referred-to-as-‘target-type’) section.

###### **Design & configure cache key (CK)** {#design-&-configure-cache-key-(ck)}

This is exactly the sameexactly same as what’s outlined in [this](#design-and-configure-cache-key-\(ck\)) section.

###### **Create a value provider** {#create-a-value-provider-1}

This is exactly the sameexactly same as what’s outlined in [this](#create-a-value-provider) section. 

###### **Whitelist target type(s) for deserialization** {#whitelist-target-type(s)-for-deserialization-1}

This is exactly the sameexactly same as what’s outlined in [this](#whitelist-target-type\(s\)-for-deserialization) section. 

###### [**(Optional) Add target-type to L1 cache consumers whitelist**](#\(optional\)-add-target-type-to-l1-cache-consumers-whitelist) {#(optional)-add-target-type-to-l1-cache-consumers-whitelist-1}

This is exactly the sameexactly same as what’s outlined in [this](#\(optional\)-add-target-type-to-l1-cache-consumers-whitelist) section.

###### **Design & configure object version key (OVK)** {#design-&-configure-object-version-key-(ovk)}

Design the OVK(s) structure based on the principles (and example use cases) described in [this](#invalidation-strategy-design) section. Once the OVK(s) are structured, there are multiple ways to construct OVK \- 

* Using shared.scalecache.services.ScaleCacheUtilWrapper

This wrapper is a spring component and can be autowired \- 

@Autowired  
private ScaleCacheUtilWrapper scaleCacheUtilWrapper  
.  
.  
GenericObjectVersionKey ovk \= scaleCacheUtilWrapper.getDefaultOVKBuilder(orgId);  
//Use append15or18CharId() for appending (standard) sfdc 15/18 char id(s)  
//Use appendNonId() for appending non-sfdc-id value(s)

The above wrapper can also be instantiated via ProviderFactory \-   
GenericObjectVersionKey ovk \= ProviderFactory.get().get(ScaleCacheUtilWrapper.class).getDefaultOVKBuilder(orgId);

* Using shared.scalecache.services.CacheKeyServices  
  This is an abstract class and therefore requires cache consumer service/class to extend this service, in order to leverage its features. This service is designed to provide ability to a consumer to validate cache hit within a functionalwithin functional test scenario, see this section for further details. If a consumer is able to extend this class, it can also be used to construct the OVK(s) as follows \-   
  GenericObjectVersionKey ovkWithParentField \= initOVK(orgId)  
    
  Note that to perform cache hit validation within an functional test (ftest) for a given cache object type is optional, since the core/underlying sub-system already has a thorough coverage via scale cache platform ftests and other cache consumers cache hit validation ftests.   

 

###### **Hook up OVK(s) to the corresponding write path** {#hook-up-ovk(s)-to-the-corresponding-write-path}

Scale cache offers two ways to setup cache invalidations, using OVK(s), on the write path \- 

1. Hooking into scale cache transaction observer  
2. Using scale cache invalidation API

As a general rule of thumb \- if your \[cache object \-\> list of write actions invalidating the object\] is somewhat complex (ex: a cache object has multiple distinct write paths invalidating the object) and its non-trivial to explicitly put the invalidation hooks individually across all the write paths, its best to look into using the transaction observer mechanism. This mechanism provides a common/central sub-system where a consumer can hook (and implement) its complete invalidation strategy, for (post-commit) invalidations. Example: cms-native.   
Feel free to consult with Sameer Khan and/or Community Cloud Performance engineering team for discussing your specific use case(s) and determining which of these two options to leverage. 

**Method\#1: Hooking into scale cache transaction observer**  
**Step\#1: Add entity type name(s) corresponding to the write actions that are going to be tracked via the OVK(s) defined in the above step, in** shared.scalecache.common.VersionedEntityType

Example:  
Using ManagedContentTypeVersion POJO cache design as a use case, the cache object has two associated OVKs \- a) OVK\[A\[1\]\] \= \[OrgId, TypeId\], b) OVK\[A\[2\]\] \=  \[OrgId, TypeVersionId\]  
The two entity types involved are ManagedContentType and ManagedContentTypeVersion. Given that the ManagedContentTypeVersion POJO is a composite of join between these two entity types, with ManagedContentTypeVersion being a child node of  ManagedContentType, the business requirement dictates that OVK\[A\[1\]\] needs to be incremented on CUD against any of the two entity types. OVK\[A\[2\]\] only needs to be incremented on CUD against ManagedContentTypeVersion entity. This means any CUD against ManagedContentType and/or ManagedContentTypeVersion invalidates ManagedContentTypeVersion POJO associated with that version id and type id. 

public enum VersionedEntityType {  
   .  
   ManagedContentType,  
   ManagedContentTypeVersion,  
   .  
}

**Step\#2:  Add \*UddConstants.Name to VersionedEntityType mapping in** com.scalecache.invalidation.VersionedEntityTypeMapper

Example:  
Using ManagedContentTypeVersion POJO cache design as a use case, the following are the entries in the type mapper   
static {  
     map.put(ManagedContentTypeUddConstants.Name, VersionedEntityType.ManagedContentType);  
     map.put(ManagedContentTypeVersionUddConstants.Name, VersionedEntityType.ManagedContentTypeVersion);  
        .  
}

**Step\#3: Create OVKCreator(s) for the respective entity types**   
OVKCreators are required to implement EntityObjectInvKeyCreator  
and are responsible for constructing OVK(s) associated with the given entity type. In most of the use cases, there would be 1x1 mapping between an entity type (defined and mapped in VersionedEntityType and VersionedEntityTypeMapper) and OVKCreator. The recommended practice is for the OVKs belonging to a given feature area to reside under com.scalecache.invalidation.ovk.\<feature area name\>. Example: com.scalecache.invalidation.ovk.mcontent for managed content (cms native)

Using ManagedContentTypeVersion POJO cache design as a use case, the following two OVKCreators were defined. 

For ManagedContentType \- 

public class ManagedContentTypeOVKCreator implements EntityObjectInvKeyCreator {

@Override  
public List\<CacheKey\> createInvKeys(final String orgId, final IEntityObject entityObject) {  
        List\<CacheKey\> cacheKeys \= new ArrayList\<CacheKey\>();  
        .  
        cacheKeys.add(createIdBasedInvKey(orgId, entityObject));  
        .  
        return cacheKeys;  
    }  
          
private CacheKey createIdBasedInvKey(String orgId, IEntityObject entityObject) {  
        return new GenericObjectVersionKey(orgId).append15or18CharId(entityObject.getString(ManagedContentTypeUddConstants.Fields.Id));  
    }  
}

If, within a CUD op, there involves a change in OVK member field value scenario and the use case warrants performing invalidation against the old value as well (in addition to the new value), the following code needs to be used \- 

IEntityObject.isFieldChangedSinceBeginningOfTransaction(\<field name\>)  
IEntityObject.getCopyOfOriginal().getString(\<field name\>)  
//Construct OVK using the old value

Example:   
There is a cache object in the managed content (cms native) area, caching ManagedContentType.Id key’d off of ManagedContentType.DeveloperName. Anytime there is a change in the dev name value, we need to invalidate the old association, in addition to invalidating with the new (changed) value. The associated OVK is a composite of \[OrgID, DeveloperName\] and the context entity type is ManagedContentType. Therefore, ManagedContentTypeOVKCreator is also responsible for constructing this OVK. 

Code for detecting a change in value and fetching the old value \-   
private Optional\<CacheKey\> createOldDevNameIfChangedBasedInvKey(String orgId, IEntityObject entityObject) {  
        CacheKey ovk \= null;  
        If (entityObject.isFieldChangedSinceBeginningOfTransaction(ManagedContentTypeUddConstants.Fields.DeveloperName)   
                && \!entityObject.getCopyOfOriginal().isFieldEmpty(ManagedContentTypeUddConstants.Fields.DeveloperName)) {  
            ovk \= new GenericObjectVersionKey(orgId).appendNonId(entityObject.getCopyOfOriginal().getString(ManagedContentTypeUddConstants.Fields.DeveloperName));  
        }  
        return Optional.ofNullable(ovk);  
    }

For complete code, please see ManagedContentTypeOVKCreator.java

**Step\#4: Add ‘OVKCreator(s) to VersionedEntityType’ mapping in com.scalecache.invalidation.ovk.EntityObjInvKeyCreatorsMapperImpl**

 @Override  
    public EntityObjectInvKeyCreator getInvKeyCreators(VersionedEntityType entityType) {  
          
        switch (entityType) {  
        case ManagedContentType:  
            return new ManagedContentTypeOVKCreator();  
        case ManagedContentTypeVersion:  
            return new ManagedContentTypeVersionOVKCreator();  
        .  
        default:  
            break;  
        }  
        return null;  
    }

**Method\#2: Using scale cache invalidation API**  
Scale cache also offers invalidation API that consumers can embed in the context transaction(s) write path (always post-commit) and explicitly enqueue invalidation. The API takes in list of OVK(s) (associated with the write event) as an argument. Therefore, the cache consumer still have to design the OVKs (exactly the same approach as what’s outlined above (in the scale cache transaction observer based on-boarding approach), but instead of hooking the actual invalidations to scale cache transaction observer based system, they will have to explicitly call this API in all the involved write paths. 

The invalidation API is \-   
com.scalecache.invalidation.services.ScaleCacheInvalidator  
and the usage is \-   
ProviderFactory.get().get(ScaleCacheInvalidator.class).invalidate(Lists.newArrayList(ovk));

**Again, the invalidation API OR Scale Cache transaction observer based invalidation strategy is ONLY usable from within core tier.** 

Example\#1:  
Account Relationship feature uses this API for their cache invalidations. See AccountRelationshipAccountToInfoScaleCacheService\#invalidate 

@Override  
    public void invalidate(CacheableKey key) {  
        GenericObjectVersionKey ovk \= initOVK(key.getOrganizationId())  
                .append15or18CharId(key.getKey1()); //AccountToId  
        // Check whether its single-key or multiple-keys invalidation. multiple-keys \== \[AccountToId, \<All associated types\>\],   
        // which for scale cache means invalidate using \*just\* the AccountToId  
        if (\!key.getKey2().equals(CDistributedCache.WILDCARD\_VALUE)) {  
            ovk.appendNonId(key.getKey2()); //Type  
        }  
          
        ProviderFactory.get().get(ScaleCacheInvalidator.class).invalidate(Lists.newArrayList(ovk));  
    }

The below trace snapshot shows the call path for the above (example) invalidation hook, for ‘save’ and ‘update’ against ARAccountTypeJunc BPO entity.   
![][image1]

Example\#2:  
Knowledge Articles in Communities use this API for cache invalidations. See ArticleChangeNotificationServiceForScaleCacheImpl\#sendArticlesInfo

Please note that at the time of writing this piece of code, the GenericObjectVersionKey service did not existed therefore we had to write OVK classes custom to Knowledge Article implementation (KnowledgeArticleVersArticleIdBasedInvKey and KnowledgeArticleVersUrlBasedInvKey). At this point, with the existence of GenericObjectVersionKey, there is no need for any consumer to write custom classes for their OVKs. For more details on that use case usage of scale cache design, please see [this](https://docs.google.com/document/d/1AhLODS5ZE0lOiQDjxeIJbwtgubosUT_L1h5qvZA7uOg/edit) document.    
    
@Override  
public void sendArticlesInfo(List\<ArticleChangeNotificationDataHolder\> articlesInfo) {  
        ....  
              
            try {  
                **scaleCacheInvalidator.invalidate(objectVersionKeys)**;  
            } catch (Exception e) {  
                /\*defensive code to ensure we do not break the parent transaction. we log explicit gacks lower in the stack\*/  
                BaseSfdcGack gack \= new BaseSfdcGack(GackLevel.SEVERE,   
                        "ArticleChangeNotificationServiceForScaleCache\#sendArticlesInfo",  
                        String.format("Exception while attempting to invalidate knowledge article related entr(ies) for scalecache, orgId: %s",   
                                UserContext.get().getOrganizationId()),   
                        new Exception().fillInStackTrace());     
                gack.send();     
            }  
        }  
    }

The below trace snapshot shows the call path for the above (example) invalidation hook, for ‘save’ and ‘update’ against ARAccountTypeJunc BPO entity. 

![][image2]

###### **Use of Scale Cache getValue() APIs to consume cache service** {#use-of-scale-cache-getvalue()-apis-to-consume-cache-service-1}

All of the publicly consumable read APIs are exposed via ScaleCacheUtilWrapper.java  
For using the get() against \[TTL \+ Object state change based invalidation \+ sync refresh\] config, following are the APIs \- 

\#getValue(CacheKeyBuilder\<T\> cacheKeyBuilder, ValueProvider\<T\> valueProvider, ObjectVersionKey invalidationVersionKey)

\#/\* Read-only api \- attempts to fetch the value from cache. If the value does not exist in the cache OR is invalidated, returns null \*/  
\#getValueWithNoLoad(CacheKeyBuilder\<T\> cacheKeyBuilder, List\<ObjectVersionKey\> invalidationVersionKeyList)

Example\#1 \- ManagedContentTypeVersion POJO cache:  
See ManagedContentSetupCachingUtilImpl.java for complete code  
@Override  
    public ManagedContentTypeVersion getTypeVersionById(@NonNull String versionId)  
            throws ScaleCacheServiceException {  
       ...  
        CacheKeyBuilder\<ManagedContentTypeVersion\> managedContentTypeVersionBuilder \= initCacheKey(ManagedContentTypeVersion.class, AppVersion.CURRENT,   
                getTypeVersionByIdMethod, versionId);  
          
        ValueProvider\<ManagedContentTypeVersion\> valueProvider \= new AbstractTimeoutBasedValueProvider\<ManagedContentTypeVersion\>(ScaleCacheUtilWrapper.MEDIUM\_TTL\_SECS) {  
            @Override  
            protected ManagedContentTypeVersion computeValue() throws ScaleCacheServiceException {  
                ManagedContentTypeVersion value \= null;  
                try {  
                    ...  
                    }  
                } catch (Exception e) {  
                    throw new ScaleCacheServiceException(e.getMessage(), e);  
                }  
                return value;  
            }  
        };  
        ManagedContentTypeVersion mcTypeVersion \= ProviderFactory.get().get(ScaleCacheUtilWrapper.class).getValue(managedContentTypeVersionBuilder, valueProvider,   
                buildOVKsForManagedContentTypeVersion(versionId));  
        return mcTypeVersion \!= null ? mcTypeVersion : null;  
    }

Example\#2 \- ArticleVersionId cache:  
See CommunityUIUtilImpl.java for complete code  
private String getArticleVersionIdByArticleId(String networkId, String articleId, String lang) throws ServiceException {  
        ...  
        // Cache articleId to articleVersionId mapping  
        GlobalCachingGuestFeature feature \= GlobalCachingGuestFeature.ArticleVersionForUrlLang;  
        final CacheKeyBuilder\<ArticleVersionId\> idCacheKey \= initCacheKey(ArticleVersionId.class, AppVersion.CURRENT, qualifiedGetArticleFromArticleIdMethodForGuest, feature);   
         
        idCacheKey.append(articleId);  
        idCacheKey.append(language);  
          
        ValueComputer\<ArticleVersionId\> idValueProviderFactory \= new ValueComputer\<ArticleVersionId\>() {

            @Override  
            public ArticleVersionId computeValue(AtomicInteger cacheHitFlag) throws ServiceException {  
                ...  
        };  
          
        KnowledgeArticleVersArticleIdBasedInvKey knowledgeArticleVersArticleIdBasedInvKey \= null;  
        if (scaleCacheUtilGateEvaluator.isGateEnabled(GaterGatesScaleCache.ENABLE\_SCALE\_CACHE\_OV\_INV\_SCHEME\_FOR\_KARTICLES)) {  
            knowledgeArticleVersArticleIdBasedInvKey \= new KnowledgeArticleVersArticleIdBasedInvKey(orgService.getId(),   
                    articleId);  
            idCacheKey.append(ObjectVersionKey.OVK\_UNIQUE\_IDENTIFIER);  
        }  
        ArticleVersionId articleVersionIdHolder \= provideValueWithCaching(networkId, feature, idCacheKey, knowledgeArticleVersArticleIdBasedInvKey, idValueProviderFactory);  
        ...  
    }

Example\#3 \- AccountRelationshipAccountToInfo cache:  
See AccountRelationshipAccountToInfoScaleCacheService.java for complete code

@Override  
public AccountRelationshipAccountToInfo get(CacheableKey key) throws SQLException {          
       ...  
          
final CacheKeyBuilder\<AccountRelationshipAccountToInfo\> cacheKeyBuilder \= initCacheKey(AccountRelationshipAccountToInfo.class, AppVersion.CURRENT, getNameMethod,   
                orgId, Optional.ofNullable(null), accountToId, type);  
          
        ValueProvider\<AccountRelationshipAccountToInfo\> valueProvider \= new AbstractTimeoutBasedValueProvider\<AccountRelationshipAccountToInfo\>(ScaleCacheUtilWrapper.MEDIUM\_TTL\_SECS) {  
            @Override  
            protected AccountRelationshipAccountToInfo computeValue() throws ScaleCacheServiceException {  
               ...  
            }  
        };  
          
        AccountRelationshipAccountToInfo arAccountToInfo \= null;  
        try {  
            GenericObjectVersionKey ovkWithParentField \= initOVK(orgId).append15or18CharId(accountToId);  
            GenericObjectVersionKey ovkWithAllFields= initOVK(orgId).append15or18CharId(accountToId).appendNonId(type);  
            List\<ObjectVersionKey\> ovkList \= Lists.newArrayList(ovkWithAllFields, ovkWithParentField);  
            arAccountToInfo \= ProviderFactory.get().get(ScaleCacheUtilWrapper.class).getValue(cacheKeyBuilder, valueProvider, ovkList);  
        ...  
    }

### 

### [**Cache ASYNC Refresh**](#heading=h.yobnmgsuv7xd) {#cache-async-refresh}

This cache object refresh type involves building/loading (from primary data-source) a given cache object in async fashion, leveraging sfdc MQ framework. The consumers of this approach are use cases where loading a cache object from primary datasource is expensive enough that it can NEVER be undertaken in a foreground thread. Our first use case of this was Server Side Rendering (SSR) feature in communities. As with Scale Cache in general, the cache consumer of this could be either in UI tier or Core. The ASYNC refresh request message is enqueued from the consumer tier to Core via an invocable action. 

#### **Design** {#design}

![][image3]

In the above diagram \- 

* The to-be-cached component (in UI tier) invokes a call to ScaleCacheUtilWrapper\#getValueWithAsyncRefresh(...) api to fetch the cache object.   
* Scale cache invokes the REST api call. The REST api calls the respective component message handler that enqueues the message and returns immediately.  
* On dequeue, the handler calls ScaleCacheUtilWrapper\#buildCache api which takes the cacheKeyBuilder, valueProvider and caches the value for the specific key.  
* Any subsequent request would fetch the data from cache.

#### **On-boarding steps** {#on-boarding-steps-2}

##### **Create a cache object wrapper, referred to as ‘target-type’**   {#create-a-cache-object-wrapper,-referred-to-as-‘target-type’-2}

This is exactly same as what’s outlined in [this](#create-a-cache-object-wrapper,-referred-to-as-‘target-type’) section.

##### **Design & configure cache key (CK)** {#design-&-configure-cache-key-(ck)-1}

This is exactly same as what’s outlined in [this](#design-and-configure-cache-key-\(ck\)) section.

##### **Whitelist target type(s) for deserialization** {#whitelist-target-type(s)-for-deserialization-2}

This is exactly same as what’s outlined in [this](#whitelist-target-type\(s\)-for-deserialization) section.

##### **(Optional) Add target-type to L1 cache consumers whitelist** {#(optional)-add-target-type-to-l1-cache-consumers-whitelist-2}

This is exactly same as what’s outlined in [this](#\(optional\)-add-target-type-to-l1-cache-consumers-whitelist) section. 

##### **Create a Scale Cache ACTION TYPE ENUM** {#create-a-scale-cache-action-type-enum}

public enum ScaleCacheActionType implements EnumItem, EnumItemCommon {

**MY\_SCALE\_CACHE\_ACTION("myActionScaleCacheAction", AppVersion.VERSION\_214);**

String apiValue;  
private AppVersion minApiVersion;  
private static final Map\<String,ScaleCacheActionType\> FROM\_APINAME;

static {  
Builder\<String,ScaleCacheActionType\> fromApiName \= ImmutableMap.builder();  
for(ScaleCacheActionType type : ScaleCacheActionType.values()) {  
fromApiName.put(type.getApiValue().toLowerCase(), type);  
}

FROM\_APINAME \= fromApiName.build();  
}...

##### **Map ACTION ENUM to the corresponding message handler** {#map-action-enum-to-the-corresponding-message-handler}

public class ScaleCacheActionServiceImpl implements ScaleCacheActionService{

private static final EnumMap\<ScaleCacheActionType, ScaleCacheAction\> SUPPORTED\_ACTIONS;

static {  
EnumMap\<ScaleCacheActionType, ScaleCacheAction\> actions \= Maps.newEnumMap(ScaleCacheActionType.class);  
// loop to make sure we don't miss newly added actions at compile time  
for (ScaleCacheActionType type : ScaleCacheActionType.values()) {  
switch(type) {  
//Add a case for your enum and the message handler class name  
case **MY\_SCALE\_CACHE\_ACTION**:  
**actions.put(type, ProviderFactory.get().getBean(ScaleCacheAction.class, "myScaleCacheActionMessageHandlerAction"));**  
break;  
default:  
break;  
}  
}  
SUPPORTED\_ACTIONS \= actions;  
}

##### **Add a bean for the message handler** {#add-a-bean-for-the-message-handler}

@Configuration  
public class ScaleCacheServiceConfig {  
@Bean  
@Lazy  
public ScaleCacheAction **myScaleCacheActionMessageHandlerAction**() {  
return new **MyScaleCacheActionMessageHandlerAction**();  
}  
}

##### **Create a message handler** {#create-a-message-handler}

public class MyScaleCacheActionMessageHandlerAction implements ScaleCacheAction {

// MQ PARAMS Keys  
public static final String MQ\_PARAM\_MY\_CUSTOM\_PARAM \= "myCustomParam";

@Autowired  
private ScaleCacheUtilWrapper scaleCacheUtilWrapper;  
@Autowired  
private CoreServicesScaleCacheLogger coreServicesScaleCacheLogger;

@Override  
public boolean handleMessage(Map\<String,String\> parameters) {  
boolean isCacheBuildSuccessful=false;

//Scale cache specific parameters  
String orgId=parameters.get(AsyncRefreshDataWrapper.ORG\_ID);  
Optional\<String\> networkId=Optional.ofNullable(parameters.get(AsyncRefreshDataWrapper.NETWORK\_ID));  
Optional\<String\> targetType=Optional.ofNullable(parameters.get(AsyncRefreshDataWrapper.TARGET\_TYPE));  
String cacheKey=parameters.get(AsyncRefreshDataWrapper.CACHE\_KEY);  
String cacheTTLSeconds=parameters.get(AsyncRefreshDataWrapper.CACHE\_TTL);

//Component specific parameters  
String myParam=parameters.get(MQ\_PARAM\_MY\_CUSTOM\_PARAM);

final ValueProvider\<String\> valueProvider=createValueProvider(myParam,Integer.parseInt(cacheTTLSeconds));  
try {  
//Build value for the cache key  
**isCacheBuildSuccessful=scaleCacheUtilWrapper.buildCache(orgId, networkId, targetType, cacheKey, valueProvider);**  
} catch (ScaleCacheServiceException x) {  
coreServicesScaleCacheLogger.logScgen(orgId, CoreServicesScaleCacheLogger.MsgType.ERROR, this.getClass().getCanonicalName(), "Error while building cache key value for component",  
String.format("Stacktrace:%s", StackUtils.getStackTrace(x)));  
}  
return isCacheBuildSuccessful;

}

private ValueProvider\<String\> createValueProvider(String myParam, int cacheTTLSeconds) {  
return new AbstractTimeoutBasedValueProvider\<String\>(cacheTTLSeconds) {  
@Override  
protected String computeValue() {  
//Write the logic to compute the value  
....  
return "value";  
}  
};  
}  
@Override  
public Map\<String, String\> getActionEnumOrId(List\<String\> names) {  
// TODO Auto-generated method stub  
return null;  
}

@Override  
public boolean hasAccess() {  
return true;  
}

@Override  
public ScaleCacheActionType getType() {  
**return ScaleCacheActionType.MY\_SCALE\_CACHE\_ACTION;**  
}

@Override  
public QueueMessage createQueueMessage(String cacheKeyBuilder, Map\<String,String\> resourceParameters) {  
Map\<String, String\> parameters \= new HashMap\<String, String\>();

//Add the cache key builder to map  
parameters.put(AsyncRefreshDataWrapper.CACHE\_KEY, cacheKeyBuilder);  
// Get parameters  
String orgId \= resourceParameters.get(AsyncRefreshDataWrapper.ORG\_ID);  
parameters.putAll(resourceParameters);

QueueMessage queueMessage \= new QueueMessageImpl(orgId, MessageQueueTypeEnum.SCALE\_CACHE\_ASYNC\_REFRESH,  
parameters);

return queueMessage;  
}  
}

##### **Use of Scale Cache getValue() APIs to consume cache service** {#use-of-scale-cache-getvalue()-apis-to-consume-cache-service-2}

All of the publicly consumable APIs are exposed via ScaleCacheUtilWrapper.java \- 

ScaleCacheUtilWrapper\#getValueWithAsyncRefresh  
**Parameters**:  
restConnection \- Accepts a rest request object and executes the request  
cacheKeyBuilder \- Contains key parameters to identify the request  
asyncRefreshDataWrapper \- Wrapper for parameters to build Value Provider  
**Returns**:  
returns value if cached, else null  
**Throws**:  
ScaleCacheServiceException

**Example:**  
class MyUITierClass {  
void foo() {  
String myData \= "myData";  
String orgId \= "0xOrgId";  
String networkId \= "0xNetworkId";

CacheKeyBuilder\<String\> cacheKeyBuilder \= scaleCacheUtilWrapper.getCacheKeyBuilder(String.class,  
AppVersion.CURRENT, METHOD);

// required  
cacheKeyBuilder.appendOrgId(orgId);  
cacheKeyBuilder.appendNetworkId(networkId);  
final int cacheTTLSeconds \= 86400; // Set TTL to 1 day  
Class\<String\> targetType \= cacheKeyBuilder.getTargetType();

Map\<String, String\> resourceParameters \= new HashMap\<String, String\>();

// my custom fields  
resourceParameters.putIfAbsent("myData", myData);

try {  
**return scaleCacheUtilWrapper.getValueWithAsyncRefresh(restConnection, cacheKeyBuilder,**  
**new AsyncRefreshDataWrapper(resourceParameters));**  
} catch (ScaleCacheServiceException x) {  
.....  
}  
}

### **Counter Tracker Service** {#counter-tracker-service}

#### **Design** {#design-1}

This service is built upon the following underlying CaaS APIs, exposed via FancyCacheClient

* \#getCounterValue()  
* \#incrementCounter()  
* \#incrementCounterIfExists()  
* \#decrementCounterIfExists()

It’s by-design that counter is only initialized on read path (\#getCounterValue), and never on write path (\#incr/\#decr). There are several reasons for this pattern \- 

* Initializing a counter can be an expensive operation and therefore is a bad design choice  for it to be executed (as a default) on a write path. This is particularly important if the incr/decr hooks (either manually placed OR using (indirectly) via scale cache transaction observer) are in latency sensitive transactions and impacting broader set of teams. For example: there could exist use cases where counter tracker service consumer scope is only a subset of the usage surface area of that entity. And the consumer use case may tolerate counter init latency but exposing it to the entire surface area (upon entity INSERT/DELETE) is at minimum un-necessary and may even be unacceptable. This design choice falls under our general design pattern of not introducing high latency calls in the scale cache transaction observer based hooks.  
  * If a given counter key does not exist in CaaS, the \#incr/\#decr ops would be no-ops.   
    * This is not an issue for use cases where the counters are backed by a persistent storage (such as db). In those cases, there is no loss of data, since the updated counts will always be reconstructed (on read path) via the primary datasource, upon invoking \#getValueCounter.   
    * Depending on the usage pattern, this may (or may not) be an issue with transient in-memory counters (not backed by a persistent storage).   
      * A low read / high write scenario is most susceptible to loss counts. A counter cache key may get explicitly evicted or TTL expired and failing any subsequent read, may result in loss tracking of counter incr/decr operations all the way upto the next read cycle, OR next eviction. One option available for this consumption pattern is to NOT use scale cache transaction observer based incr/decr ops and instead manually place \#incr/\#decr calls **paired** with \#getValueCounter (\#getValueCounter first, followed by \#incr or \#decr) in the relevant write paths IF the entire surface area of the use cases for that entity can absorb any potential latency involved with init’ing a given counter.   
      * A high read / low write scenario is least susceptible to loss counts, since a count upon cache miss will be init’ed upon read  
      * A low read / low write scenario would fall somewhere in between the above \[min,max\] bounds and will need to be examined on case-by-case basis for determining the effectiveness of this solution   
      * If a (in-memory based counter) use case has minimal tolerance of loss counts AND has a low read / high write scenario AND has a high cost of init’ing a counter AND all the surface area use cases associated with that entity canNOT absorb that latency on every write, this solution is NOT for that use case.  

#### **On-boarding Steps** {#on-boarding-steps-3}

**Note**: The counters are by default persisted in L3 unless explicitly added in the block list for L3 counters. L3 don’t honorhonner TTL, so on L2 expiry the counter will be copied into L2 from L3.

##### **Create a counter wrapper, referred to as ‘target-type’** {#create-a-counter-wrapper,-referred-to-as-‘target-type’}

In this case, this wrapper obj. is an empty class, used solely for logging purposes, to identify and filter scale cache loglines against particular consumer of this service. The wrapper obj is NOT cached in CaaS, since the cache obj is always of type Long. The recommended best practice is to create this empty class under shared-scalecache module, within shared.scalecache.dataholders.*\<consumer tag\>*. This becomes a requirement if the consumer plans to leverage scale cache transaction observer based system to perform \#incr/\#decr upon write actions, which we recommend as a preferred approach, as oppose to manually putting ScaleCacheUtilWrapper\#incr/\#decr APIs in all the involved write paths. 

Example (Reference: AccountRelationshipsCountInOrg.java)

/\*\*  
 \* This is an empty class by-design, purely for logging purpose use, as target-type name in the CacheKeyBuilder   
 \* @author sameer.khan  
 \* @since 218  
 \*/  
public class AccountRelationshipsCountInOrg {}

##### **Design & configure counter cache key (CCK)**  {#design-&-configure-counter-cache-key-(cck)}

This is nearly same as what’s outlined in [this](#design-and-configure-cache-key-\(ck\)) section

Example (Reference: AccountRelationshipFunctions.java)

final CacheKeyBuilder\<Long\> arCountCKBuilder \= ProviderFactory.get().get(ScaleCacheUtilWrapper.class).getCacheKeyBuilder(AccountRelationshipsCountInOrg.class, AppVersion.CURRENT);      arCountCKBuilder.appendOrgId(UserContext.get().getOrganizationId());

##### **Create a counter value provider** {#create-a-counter-value-provider}

The value provider will be the source supplying the initial counter value whenever the associated counter-cache-key is absent in CaaS. The value provider needs to extend from CounterValueProvider, and can only return type long. The \#getNewValue should return \> 0 value. 

Example (Reference: AccountRelationshipFunctions.java)

CounterValueProvider valueProvider \= new CounterValueProvider(ScaleCacheUtilWrapper.MEDIUM\_TTL\_SECS) {  
            @Override  
            public Long computeValue() throws ScaleCacheServiceException {  
                try {  
                    return (long) computeArCountInOrg();  
                } catch (AxisFault af) {  
                    throw new RuntimeException(af);  
                } catch (SQLException s) {  
                    throw new ScaleCacheServiceException(s.getMessage(),s);  
                }  
            }  
        };

##### **Hook up counter \#incr/decr operations to the corresponding write path** {#hook-up-counter-#incr/decr-operations-to-the-corresponding-write-path}

Scale cache offers two ways to setup this, on the write path \- 

1. Hooking into scale cache transaction observer  
2. Directly utilizing scale cache \#incr/\#decr API’s via manual injection on all the write path    

\#1 is the preferred and recommended way to setup counter increments/decrements, unless a given use case warrants otherwise. Please consult with Sameer Khan and/or Community Cloud Performance engineering team for recommendations on the given use case

###### **Method\#1: Hooking into scale cache transaction observer** {#method#1:-hooking-into-scale-cache-transaction-observer}

**Step\#1: Add entity type name(s) corresponding to the write actions that are going to trigger counter increment/decrement operations, in** shared.scalecache.common.VersionedEntityType

Example (for AccountRelationship records count use case) (Reference: VersionedEntityType.java)  
public enum VersionedEntityType {  
      ...  
AccountRelationship;  
}

**Step\#2:  Add \*UddConstants.Name to VersionedEntityType mapping in** com.scalecache.invalidation.VersionedEntityTypeMapper  
   
Example (for AccountRelationship records count use case) (Reference: VersionedEntityTypeMapper.java)

static {  
        ...  
        map.put(AccountRelationshipUddConstants.Name, VersionedEntityType.AccountRelationship);  
    }  
  

**Step\#3: Create CounterTrackerKeyCreator(s) for the respective entity types**  
These key creator(s) are required to implement EntityObjectCounterTrackerKeyCreator and are responsible for constructing counter-tracker cache key(s) associated with the given entity type. 

Using AccountRelationship records count use case as an example, the following key creator was defined \- 

**Example (Reference:** AccountRelationshipCounterTrackerKeyCreator.java**)** 

/\*\*  
 \*  Counter Tracker Key creator for AccountRelationships count in an org.   
 \*  The key is based off of OrgId  
 \* @author sameer.khan  
 \* @since 218  
 \*/  
public class AccountRelationshipCounterTrackerKeyCreator implements EntityObjectCounterTrackerKeyCreator {

    @Override  
    public List\<CounterTrackerServiceOpInfoWrapper\> createCounterTrackerKeys(String orgId, IEntityObject entityObject,  
            DmlType dmlType) {  
          
        List\<CounterTrackerServiceOpInfoWrapper\> counterTrackerKeys \= new ArrayList\<CounterTrackerServiceOpInfoWrapper\>();  
        if (dmlType.equals(DmlType.INSERT)) {  
            CounterTrackerServiceOpInfoWrapper counterTrackerInfoWrapper \= new CounterTrackerServiceOpInfoWrapper  
            (CounterTrackerOpTypes.Incr, createCacheKeyBuilder(orgId), 1);  
            counterTrackerKeys.add(counterTrackerInfoWrapper);  
        }  
          
        if (dmlType.equals(DmlType.DELETE)) {  
            CounterTrackerServiceOpInfoWrapper counterTrackerInfoWrapper \= new CounterTrackerServiceOpInfoWrapper  
            (CounterTrackerOpTypes.Decr, createCacheKeyBuilder(orgId), 1);  
            counterTrackerKeys.add(counterTrackerInfoWrapper);  
        }  
          
        return Collections.unmodifiableList(counterTrackerKeys);  
    }  
      
    private CacheKeyBuilder\<Long\> createCacheKeyBuilder(String orgId) {  
        final CacheKeyBuilder\<Long\> arCountCKBuilder \= ProviderFactory.get().get(ScaleCacheUtilWrapper.class).getCacheKeyBuilder  
                (AccountRelationshipsCountInOrg.class, AppVersion.CURRENT);  
        arCountCKBuilder.appendOrgId(orgId);  
        return arCountCKBuilder;  
    }  
}

**Step\#4: Add ‘CounterTrackerKeyCreator(s) to VersionedEntityType’ mapping in** com.scalecache.counter.tracker.EntityObjectCounterTrackerKeyCreatorsMapperImpl

Using AccountRelationship records count use case as an example \-   
**Example (Reference:** EntityObjectCounterTrackerKeyCreatorsMapperImpl  
.java**)**

/\*\*  
\* Counter-tracker key creators mapper service  
\*  
\* @author sameer.khan  
\* @since 218  
\*/  
@Component  
@Lazy  
public class EntityObjectCounterTrackerKeyCreatorsMapperImpl implements EntityObjectCounterTrackerKeyCreatorsMapper {

    @Override  
    public EntityObjectCounterTrackerKeyCreator getKeyCreators(VersionedEntityType entityType) {  
          
        switch (entityType) {  
        case AccountRelationship:  
            return new AccountRelationshipCounterTrackerKeyCreator();  
        default:  
            break;  
        }  
        return null;  
    }  
}

###### **Method\#2: Directly utilizing scale cache \#incr/\#decr API’s via manual injection in the write path(s)** {#method#2:-directly-utilizing-scale-cache-#incr/#decr-api’s-via-manual-injection-in-the-write-path(s)}

This is a non-preferred/fallback alternate to method\#1, where method\#1 cannot be used. The reason method\#1 is preferred is because it provides a common infrastructure to simply plug-in counter increment/decrement (post-commit) operations on the write path for the context entity type, without needing to explicitly inject these operations in all the required places within the consumer code, for post-commit execution.

However, if need be, Scale cache provides ScaleCacheUtilWrapper\#incrCounterValueIfExists and \#decrCounterValueIfExists APIs that consumers can embed in the context transaction(s) write path (always need be post-commit) and explicitly perform counter increment/decrement operations. The API takes in CacheKeyBuilder and byValue as args. The consumer still have to code the counter cache key(s) (exactly the same approach as what’s outlined above (in the scale cache transaction observer based on-boarding approach), but instead of hooking them into the scale cache transaction observer based system, they will have to explicitly call these APIs in all the involved write paths, post commit. 

##### **Use of Scale Cache getCounterValue() APIs to consume counter-tracker service**  {#use-of-scale-cache-getcountervalue()-apis-to-consume-counter-tracker-service}

ScaleCacheUtilWrapper\#getCounterValue is the API to consume in order to fetch counter value associated with the given counter-cache-key. 

/\* Get the current counter-value associated with the supplied cache-key. In case the value object is absent in CaaS, it will load/compute it   
     \* using the supplied value provider, followed by initializing it in CaaS \*/  
    long getCounterValue(CacheKeyBuilder\<Long\> cacheKeyBuilder, CounterValueProvider valueProvider) throws ScaleCacheServiceException;

Example (Reference: AccountRelationshipFunctions\#getArCountInOrgFromCache)

// conversion of long to int  
        return (int) ProviderFactory.get().get(ScaleCacheUtilWrapper.class).getCounterValue(arCountCKBuilder, valueProvider);

# Loglines {#loglines}

## LogRecordType  {#logrecordtype}

### \>csgcs {#>csgcs}

This is the main record type for scale cache service and contains various info around all its key operations. See app-logging-format.xml for complete definition and fields alignment of this logline. Some key fields are explained below \- 

| Field | Description |
| :---- | :---- |
| cacheTier | cache tier \[L1(heap)/L2(CaaS)\]  |
| operation | Operation type. See CacheOperationType enum in ScaleCacheUtilLogger.java for description of the various types. Please note that the successful gets from L1/L2 are NOT logged because of unnecessary overhead (on the logging subsystem) reasons. The cache hits are computed by joining the parent (cache consumer) logline with csgcs on requestId field \- the absence of csgcs with operationType=GET indicates cache hit. See below splunk query as an example \-  index=$pod$ $orgId$ logRecordType=augen ui.discovery.components.aura.components.forceDiscovery.topics.KnowledgeablePeopleDataProviderController getKnowledgeableUsers Guest NOT \[search index=$pod$ logRecordType=csgcs $orgId$ objectType=ui.discovery.components.aura.components.forceDiscovery.topics.KnowledgeableUsers cacheTier=L2 | stats count by requestId | table requestId\] | stats count Note: in 218, we have a story ([W-5266116](https://gus.lightning.force.com/lightning/r/ADM_Work__c/a07B0000005QMcLIAW/view)) to track and (periodically) log cache hit ratio (via csgcs), on per target-type per-org basis The scenarios that result in csgcs logline for operationType=GET are: L1 cache for the context target type is not initialized yet. Purely informational INFO. We employ lazy initialization for L1 cache, it’s init’d upon the first PUT request for the given target-type.  L1 cache: cache value was stale and needs refresh from L2 (or primary data source) L1 cache: cache value not found and will be attempted from L2 (or primary data source) L2 cache: cache value was not found and will be attempted from primary data source  |
| operationStatus | SUCCESS/FAILURE |
| reasonForOpFailure | reason for failure |
| cacheStatus | See field desc in app-logging-format.xml |
| elapsedTime | Operation runtime (msecs). Only computed for cache build or wait\_for\_build operation |

Also see cacheLogger.logCacheStats(...) in ScaleCacheUtilGlobalCacheImp.java for extract verbage of the logging for various scenarios. 

### \>scgen {#>scgen}

This logRecordType is for un-structured logging info. Info that is not mappable to the structured csgcs logRecordType. See app-logging-format.xml for complete definition and fields alignment of this logline. Some examples of this logRecordType use \- 

* (explicit) invalidation of an entry in L1   
* Sampling based logging of L1 cache hit/miss count stats per target-type, using its CacheStats service  
* Any exceptions or misc errors/issues, such as   
  * Problem initializing a given CaaS client  
  * Problem initializing OVK (Object Version Key) in CaaS  
* OVK version check resulting in a mismatch, triggering fresh cache object load from the primary data-source  
* Initialization of L1 cache for a given target-type

Also see cacheLogger.logScgen(...) in ScaleCacheUtilGlobalCacheImp.java for extract verbage of the logging for various scenarios described above.   
   
scgen is a shared logRecordType that is used by scale cache shared and core-only services. For core-only services, it’s used (via CoreServicesScaleCacheLogger\#logScgen) for the following \- 

* Any exceptions or misc errors/issues such as   
  * Failure in incrementing an OVK version\#. See ScaleCacheInvalidatorImpl\#invalidate for exact verbiage of the log content  
  * Failure in incrementing an OVK version\# within ScaleCacheInvalidationRetryMQHandler (responsible for OVK version\# incr (async) retries in-case of failure in the initial (sync) attempt  
  * Errors/exceptions within SSR message handler for its use of scale cache async refresh feature. See ServerSideRenderingMessageHandlerAction  
  * Scale cache async refresh services related error/exception. See InvocableScaleCacheAsyncRefresh\#buildErrorResultWithLog

### \>scinv {#>scinv}

This logRecordType is used for logging OVK version\# increment info, also via the CoreServicesScaleCacheLogger(\#logScInv)service. See app-logging-format.xml for complete definition and fields alignment of this logline.

# Key Features Release timelines {#key-features-release-timelines}

| Key Features | GA Release | Consumers |
| :---- | :---- | :---- |
| Distributed Cache Loader | 210  | Common platform feature. (Implicitly) used by all cache consumers |
| L1 cache | 210 | Common platform feature. Gated by whitelist. A subset of cache consumers leverage it |
| TTL Only strategy | 210 | Almost all standard components within Napili template based customer communities use this |
| Minimize thread waits via extended TTL | 212 | Common platform feature. (Implicitly) used by all cache consumers |
| State change based invalidation strategy using CaaS itself | 214 | cms-native, Account Relationship (AR), Knowledge Articles within Communities |
| Shared module between ui and core tier | 214 | Common feature |
| Async (cache object) refresh | 214 | Server Side Rendering (SSR) |
| Counter-tracker service | 218 | Account Relationship (AR) |
| Bulk-fetch support  | 222 | B2B Commerce Entitlements, B2B Navigation Trees for Nav Menu  |
| Bulk-fetch via distributed cache-loader | 224 | Same as Bulk-fetch support use-cases |

# MISC {#misc}

Scale Cache splunk dashboard(s)  
[CaaS dashboard(s)](https://argus-ui.data.sfdc.net/argus/#/dashboards/1556711?start=-1d&end=-0h&dc=CHI&corePod=cs46&caasPod=cs46&clusterName=caas-cluster-1&alias=default&coreHost=*-%5Bapp%7Ccbatch%7Cdapp%7Csapp%5D*&caasHost=*-%5Bapp%7Ccbatch%7Cdapp%7Csapp%5D*&splunkslowlog=)  
[Chatter Group](https://gus.lightning.force.com/lightning/r/ADM_Scrum_Team__c/a00B0000000wOkmIAE/view)  
[Scale Cache  consumers list](https://docs.google.com/spreadsheets/d/15jJCKYYt43HT51xIodbnQaTqh4SSAvHpxiv3f0J67WA/edit#gid=8468321)  
[Async refresh supplemental document](https://salesforce.quip.com/6FbDAogGAifa)  
[Feature GA and associated consumers map document](https://docs.google.com/spreadsheets/d/15jJCKYYt43HT51xIodbnQaTqh4SSAvHpxiv3f0J67WA/edit#gid=118549578)  
[BlackTab Global Invalidation Tool](https://confluence.internal.salesforce.com/display/BLACKT/Scale+Cache+And+Vega+Cache+Global+Invalidation+tool)

[image1]: <data:image/png;base64,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>

[image2]: <data:image/png;base64,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>

[image3]: <data:image/png;base64,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>