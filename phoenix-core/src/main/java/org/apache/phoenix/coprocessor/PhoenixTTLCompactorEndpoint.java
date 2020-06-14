package org.apache.phoenix.coprocessor;

import org.apache.hadoop.hbase.coprocessor.*;
import org.apache.hadoop.hbase.regionserver.*;
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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class PhoenixTTLCompactorEndpoint extends CompactionService implements RegionCoprocessor {

    public static class PhoenixTTLAwareCompactionRequest implements CompactionRequest {

        private CountDownLatch done;
        private Scan serializedScanRequest;

        public PhoenixTTLAwareCompactionRequest(Scan serializedScanRequest, CountDownLatch finished) {
            this.serializedScanRequest = serializedScanRequest;
            this.done = finished;
        }

        public void afterExecute() {
            this.done.countDown();
        }

        public Scan getScan() {
            return serializedScanRequest;
        }

        @Override
        public Collection<? extends StoreFile> getFiles() {
            return null;
        }

        @Override
        public long getSize() {
            return 0;
        }

        @Override
        public boolean isAllFiles() {
            return false;
        }

        @Override
        public boolean isMajor() {
            return false;
        }

        @Override
        public int getPriority() {
            return 0;
        }

        @Override
        public boolean isOffPeak() {
            return false;
        }

        @Override
        public long getSelectionTime() {
            return 0;
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
            PhoenixTTLExpiredCompactionRequest request,
            Region region)
            throws NotServingRegionException, IOException {

        CountDownLatch tracker = new CountDownLatch(region.getStores().size());
        List<CountDownLatch> allDone = Lists.newArrayListWithExpectedSize(region.getStores().size());
        List<Pair<CompactionRequest, Store>>
                crs = Lists.newArrayListWithExpectedSize(region.getStores().size());
        ClientProtos.Scan destProtoScan = ClientProtos.Scan.parseFrom(request.getSerializedScanFilter());
        Scan serverScan = ProtobufUtil.toScan(destProtoScan);

        for (Store store : region.getStores()) {
            crs.add(new Pair<CompactionRequest, Store>(new PhoenixTTLAwareCompactionRequest(serverScan, tracker), store));
        }
        /*
        try {
            requestor.requestCompaction(region,
                    "Compacting all phoenix ttl expired rows for region: " +
                            region.getRegionInfo().getRegionNameAsString(), crs);
            //tracker.await();
        } catch (Exception e) { // InterruptedException e
            throw new IOException("Interrupted while waiting for compaction on "
                    + region.getRegionInfo().getRegionNameAsString()
                    + " to complete", e);
        }

        */

    }
}

