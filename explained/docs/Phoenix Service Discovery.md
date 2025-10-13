# Phoenix Service Discovery 

## Overview

Based on [Consistent Cluster Failover](https://docs.google.com/document/d/1pAlKsro-mD7nLey08oCJJhJS0Sf4LDxeX5mycZ5b4b0/edit?usp=sharing) and migration to FKP initiative for HBase/Phoenix, we need to upgrade how HBase/Phoenix is discovered by its clients. As more and more new features are coming up in FEDX and “Falcon Paved Path” initiatives, we want to make sure, our service discovery is easy, safe and versioned. This document provides an approach on how this can be done.

## Requirements

1. Zero Downtime(Dependent on replication)
2. Safe Change Compatible
3. Developer Friendly(Ease of use)
4. (FKP Migration Only) Ability to revert back to last known good without a new deployment.

## Limitations/Assumptions

1. This solution is only applicable to Falcon clusters only.
2. This solution is applicable to dual clients using sfdc-hbase-client only. We have extended this solution to near core/offcore use case based on hbase client+scone-starter.

## Current Working

[CurrentWorking.excalidraw](https://salesforce.quip.com/-/blob/WDDAAA8c82Z/ADNJsTK7YHCyW-QzeV37Qg?name=CurrentWorking.excalidraw&s=Z7RsAU3obgYN)
[Image: image.png]
### Problems with Current Approach

1. Complicated [hbase-topological.xml](https://sourcegraph.soma.salesforce.com/git.soma.salesforce.com/CARE/core-config/-/blob/core-config-prod/src/main/resources/config/settings/defaults/hbase-topological.xml) file - contains 33k lines(contains data for 1P(out of scope for this) as well). 
2. We have overrides at cell level which can possibly be replaced by the standard pattern replacement with placeholders.
3. All configs and hbase client code are packaged in a single version which might cause issues when rolling back unrelated changes in 1 bundle.
4. Doesn’t leverage Structured Config/FedX tooling(Company wide adoption push).
5. (Migration Only) Needs deployment for rollback from FKP to EKS (if required). No kill switch available.

## Proposed Solution

As we observed above that  [hbase-topological.xml](https://sourcegraph.soma.salesforce.com/git.soma.salesforce.com/CARE/core-config/-/blob/core-config-prod/src/main/resources/config/settings/defaults/hbase-topological.xml) config has entries for almost every cell(33k lines), it is not necessary for Falcon. In Falcon, we have a fixed template for zookeeper quorum

>`zookeeper-${IDX}.zookeeper-headless.hbase.hbase1a.hbase.${FD}.${FI}.${SUBSTRATE}.${SUFFIX}`

Based on above template, we can dynamically get the zookeeper quorum for Falcon instead of using the xml.


### ***But how do we migrate from EKS to FKP using this template?***

For FKP, we plan to use a slightly different template

>`zookeeper-${IDX}.zookeeper-headless-fkp-hbase2a.hbase.fkp-hbase2a.hbase.${FD}.${FI}.${SUBSTRATE}.${SUFFIX}`

So still we can dynamically get the zk quorum.


### ***Difference between ZK for CRR Listener vs the value for ZKUrl in CRR which client is using for R/W(PhoenixConnection)?
***

In our approach, we will refer to two different ZK quorums:-

1. ZK quorum used at bootstrap for getting CRR values and listening to CRR changes
2. ZK url inside the CRR with different roles like ACTIVE, STANDBY, ACTIVE_TO_STANDBY.

Currently these values for 1 and 2 are same, but we will rely on the fact that **getting the CRR value and listening to changes** can be done separately as compared to **using the zkUrl for R/W(PhoenixConnections).** This inconsistency will be temporary during the transition and can be supported as a steady state(i.e. availability will not be affected during this inconsistency between 1 and 2).

### ***Now, how can we switchover i.e. dynamically use one quorum vs another(can be eks/fkp quorum)?***

In following steps

1.  *(One time only, Optional)* We remove all the Falcon entries in [hbase-topological.xml](https://sourcegraph.soma.salesforce.com/git.soma.salesforce.com/CARE/core-config/-/blob/core-config-prod/src/main/resources/config/settings/defaults/hbase-topological.xml) after making sure that final quorum from template vs hardcoded one in xml is same. For safety, we can update our logic to first check if dynamically rendered string matches the one in xml and if not use the xml(original) and publish a metric `zk.quorum.template.mismatch`. In subsequent release, if metric is good, we can remove the xml entries for Falcon.
2. *For every switchover*
    1. Using Admin Failover Command, we update the CRRs in **the new cluster** that will be used after the proposed switchover to the same values as **existing cluster** in failover. For eg. in Phase 1 below, EKS-1a and FKP-1a will be used after the proposed switchover. But EKS-1a was already being used, we update the CRR in new FKP-1a with the same values as in EKS-1a. At this moment, core app is not listening to this CRR znode in FKP-1a.
    2. Ensure there is replication enabled such that any new clusters are also peered. For our use case, we need to ensure replication between
        1. eks-1a and eks-1b (exists currently)
        2. eks-1a and fkp-1a (need to setup)
        3. fkp-1a and fkp-1b (need to verify)
    3. Then we use [Gater](https://confluence.internal.salesforce.com/display/gater/Gater+Home) to update the template for zookeeper quorum and zookeeper dr quorum based on current phase for FKP migration. The template value can be sent as part of Gater change itself. Gater provides [ActivationStrategy](https://confluence.internal.salesforce.com/display/public/gater/Gate+Definition+Format#GateDefinitionFormat-ActivationStrategySchema) which can target specific env/FI/FD/Cell.  Please note that switching quorum will be a 2 step process, update Gater and then a core app restart can also be done(optional). ***~~At this moment core app is listening to quorums in final clusters but it still is using the existing clusters for R/W because the znode in existing cluster has the latest version.~~ Coreapp will be listening to CRRs in ZK that it is using based on Gater + Restart. Since we plan to keep the CRR values same in all clusters, restart is not mandatory.***
    4. Using Admin Failover command, then we update the CRR in existing cluster to use combination of existing and new cluster in final state. 
        1. If the phase involves flipping ACTIVE and STANDBY clusters, we update the state of current ACTIVE to ACTIVE_TO_STANDBY so that all mutations are blocked. Wait for the replication queue to drain.
        2. At this moment, **any dual client traffic should close all existing connections and start using the new cluster**.

### ***How can we use this for EKS → FKP migration?***

_**2 Phase Approach(Recommended)**_
This approach is similar as Approach 1 but we merge Phase2 and Phase3 into a single phase. The reason is that we already have eks-1a in ACTIVE_TO_STANDBY mode when we are transitioning from Phase1 to Phase2. Since, we need the same role for eks-1a in Phase2 to Phase3 transition, we combine the phases. Below diagram explains in detail:-
[PhasesV4.excalidraw](https://salesforce.quip.com/-/blob/WDDAAA8c82Z/c9g9E15kmvf-9TsFU8UYrQ?name=PhasesV4.excalidraw&s=Z7RsAU3obgYN) 


_**Tabular Transition Steps and Impact on Reads/Writes**_

|	|	|	|	|	|Failover Connection	|Parallel Connection	|
|---	|---	|---	|---	|---	|---	|---	|
|Phase/Step	|EKS-1a	|EKS-1b	|FKP-1a	|FKP-1b	|Reads	|Writes	|Reads	|Writes	|
|Current State	|ACTIVE	|STANDBY	|Unused	|Unused	|No Impact	|No Impact	|No Impact	|No Impact	|
|Phase1- Step1	|ACTIVE	|ACTIVE_TO_STANDBY	|Unused	|Unused	|No Impact	|No Impact	|No Impact	|Writes to eks-1b blocked, but eks-1a will continue	|
|Phase1-Step2	|ACTIVE	|Unused	|STANDBY	|Unused	|No Impact	|No Impact	|No Impact	|No Impact	|
|End of Phase1 - Stable State, next phase will be started manually(using Managed Ops) after bake time	|
|Phase2-Step1	|ACTIVE_TO_STANDBY	|Unused	|STANDBY	|Unused	|New Reads Blocked for X mins, ongoing will continue	|Writes will be blocked for X mins	|No Impact	|Writes to eks-1a blocked, but fkp-1a will continue	|
|Phase2-Step2	|Unused	|Unused	|ACTIVE	|STANDBY	|No Impact	|No Impact	|No Impact	|No Impact	|
|End of Phase2 - Transition Complete	|

#### Rollback for [2 Phase](https://salesforce.quip.com/Z7RsAU3obgYN#temp:C:WDDf3d6218d128f4779a0ddaa114) Approach in Detail

**Rollback after Phase1 Completed(eks-1a - Active, fkp-1a - Standby) to state before any changes(eks-1a Active, eks-1b Standby)**
After Phase 1 is completed, we have eks-1a as ACTIVE, fkp-1a as STANDBY. Previous state(before beginning any change was) eks-1a as ACTIVE and eks-1b as STANDBY.
[Rollback-Phase1.excalidraw](https://salesforce.quip.com/-/blob/WDDAAA8c82Z/kxrCU59XEgWoogrIAK75cw?name=Rollback-Phase1.excalidraw&s=Z7RsAU3obgYN) 


**Rollback after Phase2 is completed(fkp-1a - Active, fkp-1b - Standby) to state after Phase1 is complete(eks-1a - Active, fkp-1a - Standby)**
This will be a 2 Step process

1. Update fkp-1b to ATS state, change new standby to eks-1a. After this (fkp-1a is Active and eks-1a is Standby)
2. If doing 1 stabilizes things, we let it be otherwise to rollback to original state, we Flip Active and Standby. After this (eks-1a is Active and fkp-1a is Standby)

[RollbackAfterPhase2.excalidraw](https://salesforce.quip.com/-/blob/WDDAAA8c82Z/s73z50jonSeixnuFWv-Faw?name=RollbackAfterPhase2.excalidraw&s=Z7RsAU3obgYN) 
In case of complete rollback we can execute these rollback phases 1 by 1.
Reads and Writes during rollback will have same impact as during roll forward step. For parallel, we will always have 1 cluster running at all times and for failover, we will see availability impact when we flip current Active and Standby in [Rollback after Phase2 is completed(fkp-1a - Active, fkp-1b - Standby) to state after Phase1 is complete(eks-1a - Active, fkp-1a -…](https://salesforce.quip.com/Z7RsAU3obgYN#temp:C:WDD6ccf7ebd46c4433d99f645cf1)

_**Tabular Rollback Steps and Impact on Reads/Writes**_
We plan to have automated rollback in case we fail at any intermediate step, if that is not possible, we have MOs to force update CRRs is needed. For intermediate stages between Phases (e.g. Phase1-Step1, we will just go back to previous state in [Tabular Transition Steps and Impact on Reads/Writes](https://salesforce.quip.com/Z7RsAU3obgYN#temp:C:WDD33ab575e6df1488ba40a90184))

|	|	|	|	|	|Failover Connection	|Parallel Connection	|
|---	|---	|---	|---	|---	|---	|---	|
|Phase/Step	|EKS-1a	|EKS-1b	|FKP-1a	|FKP-1b	|Reads	|Writes	|Reads	|Writes	|
|Current State	|Unused	|Unused	|ACTIVE	|STANDBY	|No Impact	|No Impact	|No Impact	|No Impact	|
|RollbackPhase1-Step1	|Unused	|Unused	|ACTIVE	|ACTIVE_TO_STANDBY	|No Impact	|No Impact	|No Impact	|Writes to fkp-1b blocked, but fkp-1a will continue	|
|RollbackPhase1-Step2	|STANDBY	|Unused	|ACTIVE	|Unused	|No Impact	|No Impact	|No Impact	|No Impact	|
|Rollback Phase 1 Complete. If this state is stable, we can remain in this state	|
|RollbackPhase2-Step1	|STANDBY	|Unused	|ACTIVE_TO_STANDBY	|Unused	|New Reads Blocked for X mins, ongoing will continue	|Writes will be blocked for X mins	|No Impact	|Writes to fkp-1a blocked, but eks-1a will continue	|
|NOTE: Depending on the issue, we can rollback to Rollback Phase3-Step2 directly from this state as well and skip RollbackPhase2-Step2, Rollback Phase3-Step1 below	|
|RollbackPhase2-Step2	|ACTIVE	|Unused	|STANDBY	|Unused	|No Impact	|No Impact	|No Impact	|No Impact	|
|Rollback Phase 2 Complete. If this state is stable, we can remain in this state	|
|Rollback Phase3-Step1	|ACTIVE	|Unused	|ACTIVE_TO_STANDBY	|Unused	|No Impact	|No Impact	|No Impact	|Writes to fkp-1a blocked, but eks-1a will continue	|
|Rollback Phase3-Step2	|ACTIVE	|STANDBY	|Unused	|Unused	|No Impact	|No Impact	|No Impact	|No Impact	|

#### _3 Phase Approach with keeping 1 cluster same in old and new cluster pairs, changing 1 at a time_

**This approach ensures that parallel clients(like Vagabond) don’t observe any downtime but the con is that there are 3 rollbackable, steady steps involved.**

For EKS to FKP migration we will have three switchovers. All these switchovers can be achieved by using above approach.
[Phasesv3.excalidraw](https://salesforce.quip.com/-/blob/WDDAAA8c82Z/GGs3oTPhluZAx7qqO2Q9UA?name=Phasesv3.excalidraw&s=Z7RsAU3obgYN)

Example Illustration of Phase 1 to 2 above for Step2  in [Now, how can we switchover i.e. dynamically use one quorum vs another(can be eks/fkp quorum)?: Phoenix Service Discovery](https://salesforce.quip.com/0Kj8AVsMgzb4#temp:C:PLGfb674382ef524772b1d427994)




#### _1 Shot Approach changing both clusters at same time_

In this approach, we execute a 1 shot failover completely from eks-1a, eks-1b to fkp-1a, fkp-1b. Instead of keeping 1 common cluster between old and new quorums in CRRs, we just completely change from eks-1a,eks-1b pair to fkp-1a,fkp-1b pair by performing following steps.
[Phases-Option2.excalidraw](https://salesforce.quip.com/-/blob/WDDAAA8c82Z/oaoTInLpovzV01DKrxSKVQ?name=Phases-Option2.excalidraw&s=Z7RsAU3obgYN)

**Additional Tuning**
_Edge Case_
One of the edge case can be that when we put eks-1a and eks-1b to Active_To_Standby mode, the replication traffic **to** these clusters will also stop. That means that any writes to eks-1b will not be replicated to eks-1a. While this doesn’t affect Failover connection, this affects Parallel connection. Consider below scenario:-

1. Parallel client writes to eks-1a and eks-1b. As these are separate clusters, there will be some time difference between updating eks-1a and eks-1b CRRs in respective zk quorums in these clusters. Some writes might go to eks-1b which are blocked by eks-1a.
2. Since eks-1a is in Active_To_Standby, these writes will not be replicated(but queued up) in eks-1b to eks-1a replication queue.
3. Since these writes are not received by eks-1a, transitively they will not flow to fkp-1a.
4. When customers are reading from fkp-1a, they will not be able to see their writes(which were acknowledged by eks-1b) for sometime **until** the eks-1b replicates those writes to eks-1a which then replicates those writes to fkp-1a.

_Solution_

1. Convert eks-1b to Active_To_Standby mode first, make sure all writes to eks-1a are drained. Since eks-1a is in ACTIVE state, we will not consider this time in downtime.
2. Once eks-1b writes are drained, move eks-1a to Active_To_Standby(Downtime starts now as no writes are being accepted to either eks-1a and eks-1b)
3. This will ensure that no writes are **ONLY** written to eks-1b as in the edge case above.
4. Continue as in diagram above.

#### Comparison

|Feature	|3 Phase Approach	|2 Phase Approach	|1 Shot Approach	|
|---	|---	|---	|---	|
|Ease of Use	|Less as more steps are involved	|Medium effort	|More as we need to schedule operation at 1 time	|
|Downtime - Failover Client	|Same	|Same	|Same 	|
|Downtime - Parallel Client	|No downtime as there is 1 common cluster in old and new CRRs at all times	|No downtime as there is 1 common cluster in old and new CRRs at all times	|Yes, as we are completely moving to 2 new clusters. We'll need to block mutations for both Active and Standby	|
|New Effort Involved - Admin Failover Script	|None(Testing pending)	|Need to update Admin Failover Script(Small)	|Need to update Admin Failover Script(Small)	|
|New Effort Involved - Threadpool Management	|Same	|Same	|Same	|

### ***Cleaning up Gater***

Once we have completed phase3 ***for all clusters***, we can update the config templates to not use Gater but update default values to use  ZK1 → `fkp-hbase1a`  and ZK2 → `fkp-hbase1b`  


## Open Questions

1. Do we need restart when we update ZK quroum templates in quorum?
    1. What will be the impact in case ZK quorum that is being used for listening to CRRs is different for sometime as compared to ZK quorums of the clusters being used for R/W?
        [SplitQuorums.excalidraw](https://salesforce.quip.com/-/blob/WDDAAA8c82Z/RXwea0UTpOO14cbI2AxUgw?name=SplitQuorums.excalidraw&s=Z7RsAU3obgYN)
    2. 
    3. This will eventually converge next time coreapp cell is restarted.
    4. Thread pool handling
        1. Currently we have 2 threadpools for Phoenix operations in client. These pools are stored in a map where the key is bootstrap url. (ref: ProtectedHTableFactory)
        2. Since the key in bootstrap url (fkp-1a , fkp-1b) will be different from urls inside CRR (eks-1a, eks-1b), it might cause issues. We need to change the thread pool map’s  write and lookup to use some canonical structure which can support cluster1, cluster2 instead of URLs. This will help us not only for this project but also for Consistent Failover(as we won’t have separate classification for Primary and DR cluster, both will be ideally equivalent).
    5. (TBD) How does this affect Gridforce jobs which read data from HBase cluster?
2. Do we restart before or after? What is the fallback behavior in case of unable to connect to CRR?
    1. [Decision] We should fail in case of unable to read from CRR and not fallback to bootstrap URL.
3. Discuss 3 Phase vs 1 Shot approach.
    1. Can we convince Vagabond(parallel) to allow us for 2 minute planned downtime?
    2. In 1 shot approach, do we change both clusters to Active_To_Standby mode in parallel or in serial. (Refer edge case above)
4. When do we actually close the connections?
    1. 


Action Items

1. Prerequisites to start the failover 
    1. Now capturing in [Monitoring for FKP migration](https://salesforce.quip.com/6uH6ARXybvou)
    2. ~~Metrics to watch for~~
        1. ~~No background jobs~~
        2. ~~Ongoing Org Migration(s)~~
        3. ~~Replication Lag (Both directions, all clusters involved)~~
        4. ~~RIT~~
        5. ~~HBCK~~
        6. ~~PDB Health~~
2. Don’t fallback on bootstrap URL (Sample [PR](https://gitcore.soma.salesforce.com/core-2206/core-public/pull/162887))
    1. Restart timing doesn’t matter (pending testing)
    2. We need to ensure all CRRs on all ZKs are consistent(EKS-1a/b, FKP-1a/b)
3. Testing under load
4. [Done] State Transition Diagram for rollback scenario
5. [Decision] Use 3 Phase Approach
6. [Decision] Close connection on standby ([PR](https://github.com/apache/phoenix/pull/2128))

## Effect of Client Connection Registry

Client connection registry is significantly updating the Service discovery mechanism for clients to move from ZK based to ZKLess connections. However, there are some gaps in client connection registry:-

1. Dual Client and Failover(HA) client are not supported by ZKLess
    1. As we are moving every connection which uses sfdc-hbase-client to Failover(HA) except Vagabond(which uses dual client), ZKLess will not apply on any client.
2. ZK Listener for ClusterRoleRecord still required
    1. Core app client creates ZK Listener during bootstrap to listen to any changes in CRR. This listener will still need to exist in ZK less world.

Based on above points, recommendation is that we continue using ZK based switchover approach as we move to FKP ~~and depending on the timing of OSS support for above concerns(***for Falcon***), we can take a decision on whether to incorporate it before FKP migration vs migrate to ZKLess after.~~
[Update 01/6/2025] Based on [Enabling DualClient in CoreApp Clients](https://salesforce.quip.com/6ufDAKWZlhIR), we have 2 phases planned for supporting dual client. 

    * For Phase-1, we will essentially be overriding the rpc client connection registry changes in 254 as the rpc client connection registry will only be applicable on single client. 
    * We will still rely on `hbase.zookeeper.quorum` and `hbase.dr.zoookeeper.quorum` to discover the hbase server and crr. We plan to move this to templated form and remove hardcoded strings in core app config
    * For Phase-2, the service discovery logic will still rely on `hbase.zookeeper.quorum` and `hbase.dr.zoookeeper.quorum` . These will be used during bootstrap to fetch/listen CRR and then use that CRR for creating connection. What this means is that property  `hbase.client.bootstrap.servers` should not be needed in core app xmls.
    * We will switch to pure ZK less for the clusters after their FKP migration is complete.

## Notes on Client Connection Registry

1. Strategy from ZK to ZKLess
    1. We don’t want to introduce a new property in topological so it is put behind a flag
    2. We are not touching on server side/non-core app entities. Anything running on cluster we are not touching
2. What about near core customers?
    1. In core FI but on separate JVM
    2. ID Mapper/BRE/Radio
        1. Do they also use dual client?
        2. That should be in scope for failover client.
3. Introuced a flag which shows registry
    1. to zk/rpc/master
    2. Currently it is zk
4. For master
    1. We need master ports
        1. different for each env
    2. For master host name
        1. We can generate DNS and instead find the hostnames using templated string
    3. Using template itself is a configuration
        1. This can be used to find
5. How DR ZK is found?
    1. For DR also we have a config in 1b for template.
6. If it is master registry then we HBaseServerConfig
7. Dual and HA will not be supported by MasterConnectionRegistry
8. Dual Client
    1. Dual is not supported in OSS
    2. If we pass anything else than ZK, it will not be able to parse.
9. HAConnection
    1. We are using zk for HA profile. RPCConnectionRegistry will use zk
    2. No support for ZK
    3. No change needed in CRR for now
10. We need ZK for setting up listener for CRR in any case during bootstrap
    1. Main driver for ZKLess is meta calls (or is it ZK Connection Leaks which bloats connections and hence threads)
    2. Meta info is cached in HMaster
11. Proposal 
    1. Since template is a config, override templates via Gater and then once we are fully migrated just update templates to fkp-1a and fkp-1b



## Appendix

### Structured Config Based Design



[ProposedWorking.excalidraw](https://salesforce.quip.com/-/blob/WDDAAA8c82Z/wU8bFWSAK7szLGAZVnZ08w?name=ProposedWorking.excalidraw&s=Z7RsAU3obgYN) 
In proposed solution, we are planning to leverage [StructuredConfig](https://confluence.internal.salesforce.com/pages/viewpage.action?pageId=812713590) which can be added in a hierarchal structure(*with defaults at the root and overrides at leaves*). This will replace [hbase-topological.xml](https://sourcegraph.soma.salesforce.com/git.soma.salesforce.com/CARE/core-config/-/blob/core-config-prod/src/main/resources/config/settings/defaults/hbase-topological.xml). This will be then used in baking a config map which can be mounted on coreapp hosts. Once this is mounted, we plan to use this as SpringBoot Application Config which can be used by hbase core app client.
In addition for migration from current [hbase-topological.xml](https://sourcegraph.soma.salesforce.com/git.soma.salesforce.com/CARE/core-config/-/blob/core-config-prod/src/main/resources/config/settings/defaults/hbase-topological.xml) to structured config, we plan to use [Gater](https://confluence.internal.salesforce.com/display/gater/Gater+Home) to be able to fallback to existing config to new config. This gater integration with client can be used as a remote switch for self managed EKS → FKP migration project as well.

**Key Benefits of Structured Config:-**

1. Config is not bundled with hbase client code.
2. Versioned.
3. Hierarchal structure leading to reduction in size and not needing to override for each cell(current xml is 33k lines).
4. Feature in Falcon Paved path which is focus for Salesforce and more likely to have investments going forward.  




### FAQ (TBA)

1. Can this Structured Config be extended to other hbase client configs as well?
2. How do we rollback StructuredConfig change if needed?
3. How will this help in migration from self managed EKS to FKP? What will be the steps involved in migration?
4. How will this help in proposed HBase Addon setup?
5. How can we use the kill switch if migration goes wrong? 



