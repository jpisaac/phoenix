package org.apache.phoenix.coprocessor;

import org.apache.hadoop.hbase.coprocessor.*;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequester;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.coprocessor.generated.CompactionProtos;
import org.apache.phoenix.coprocessor.generated.CompactionProtos.CompactionService;
import org.apache.phoenix.coprocessor.generated.CompactionProtos.PhoenixTTLExpiredCompactionRequest;
import org.apache.phoenix.coprocessor.generated.CompactionProtos.PhoenixTTLExpiredCompactionResponse;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class PhoenixTTLCompactorEndpoint extends CompactionService implements CoprocessorService, RegionCoprocessor {
    private static final Logger LOG = LoggerFactory.getLogger(PhoenixTTLCompactorEndpoint.class);

    public static class PhoenixTTLCompactionTracker implements CompactionLifeCycleTracker {

        private CountDownLatch done;
        private Scan serializedScanRequest;

        public PhoenixTTLCompactionTracker(Scan serializedScanRequest, CountDownLatch finished) {
            this.serializedScanRequest = serializedScanRequest;
            this.done = finished;
        }

        public Scan getScan() {
            return serializedScanRequest;
        }

        @Override
        public void notExecuted(Store store, String reason) {
            LOG.info(String.format("PhoenixTTLCompactionTracker.notExecuted %s", store.getRegionInfo().getRegionNameAsString()));
            CompactionLifeCycleTracker.super.notExecuted(store, reason);
            done.countDown();
        }

        @Override
        public void beforeExecution(Store store) {
            LOG.info(String.format("PhoenixTTLCompactionTracker.beforeExecution %s", store.getRegionInfo().getRegionNameAsString()));
            CompactionLifeCycleTracker.super.beforeExecution(store);
        }

        @Override
        public void afterExecution(Store store) {
            LOG.info(String.format("PhoenixTTLCompactionTracker.afterExecution %s", store.getRegionInfo().getRegionNameAsString()));
            CompactionLifeCycleTracker.super.afterExecution(store);
            done.countDown();
        }

        @Override
        public void completed() {
            LOG.info(String.format("PhoenixTTLCompactionTracker.completed"));
            CompactionLifeCycleTracker.super.completed();
        }
    }

    private RegionCoprocessorEnvironment env;
    private RegionServerServices rsServices;


    @Override public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) env;
            this.rsServices = (RegionServerServices) this.env.getOnlineRegions();

            ((RegionCoprocessorEnvironment) env).getSharedData();
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
    }

    @Override public void stop(CoprocessorEnvironment env) throws IOException {

    }

    @Override
    public Iterable<Service> getServices() {
        return Collections.singleton(this);
    }
    @Override
    public Service getService() {
        return this;
    }

    @Override public void compactPhoenixTTLExpiredRows(RpcController controller,
            PhoenixTTLExpiredCompactionRequest request,
            RpcCallback<PhoenixTTLExpiredCompactionResponse> done) {

        HRegion region = (HRegion)env.getRegion();

        // ... doing some stuff, maybe:
        // make sure we have a file to on the disk - this is blocking, so we are sure current memstore
        // state made it to disk
        //region.flushcache();

        // ...
        //set a flag that we are running a special compaction
        //myCompaction = true;

        // run a compaction and wait for it to complete
        try {
            compactRegionAndBlockUntilDone(rsServices.getCompactionRequestor(),
                    request,
                    region);
            CompactionProtos.PhoenixTTLExpiredCompactionResponse.Builder builder =
                    CompactionProtos.PhoenixTTLExpiredCompactionResponse.newBuilder();
            builder.setStatus(1);
            done.run(builder.build());

        } catch (IOException e) {
            e.printStackTrace();
        }
        // ...

    }
    /**
     * Compaction the region and wait until all stores in the region have been compacted
     * @param regionInfo region to compact
     * @param major if the compaction should be a major compaction
     * @param family the family to compact. If <tt>null</tt> compacts all families.
     * @throws NotServingRegionException if the region is not hosted on this server
     * @throws IOException if we fail to complete the compaction
     */
    private void compactRegionAndBlockUntilDone(CompactionRequester requestor,
            PhoenixTTLExpiredCompactionRequest request, HRegion region)
            throws NotServingRegionException, IOException {

        CountDownLatch allDone = new CountDownLatch(region.getStores().size());
        ClientProtos.Scan destProtoScan = ClientProtos.Scan.parseFrom(request.getSerializedScanFilter());
        Scan serverScan = ProtobufUtil.toScan(destProtoScan);
        PhoenixTTLCompactionTracker tracker = new PhoenixTTLCompactionTracker(serverScan, allDone);

        try {
            for (Store store : region.getStores()) {
                HStore hstore = (HStore) store;
                LOG.info(String.format("Before.requestCompaction %s", store.getRegionInfo().getRegionNameAsString()));
                requestor.requestCompaction(region, hstore, "Compacting all phoenix ttl expired rows for region: " +
                        region.getRegionInfo().getRegionNameAsString(), Store.PRIORITY_USER, tracker, null);
                LOG.info(String.format("After.requestCompaction %s", store.getRegionInfo().getRegionNameAsString()));
            }
            allDone.await();
        } catch (Exception e) { // InterruptedException e
            throw new IOException("Interrupted while waiting for compaction on "
                    + region.getRegionInfo().getRegionNameAsString()
                    + " to complete", e);
        }

    }
}

