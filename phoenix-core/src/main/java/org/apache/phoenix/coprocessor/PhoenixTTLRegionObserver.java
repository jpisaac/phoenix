/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.coprocessor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.metrics.MetricsPhoenixCoprocessorSourceFactory;
import org.apache.phoenix.coprocessor.metrics.MetricsPhoenixTTLSource;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.apache.phoenix.util.ScanUtil.getDummyResult;
import static org.apache.phoenix.util.ScanUtil.getPageSizeMsForRegionScanner;
import static org.apache.phoenix.util.ScanUtil.isDummy;

/**
 * Coprocessor that checks whether the row is expired based on the TTL spec.
 */
public class PhoenixTTLRegionObserver extends BaseScannerRegionObserver implements RegionCoprocessor {
    private static final Logger LOG = LoggerFactory.getLogger(PhoenixTTLRegionObserver.class);
    private MetricsPhoenixTTLSource metricSource;

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        metricSource = MetricsPhoenixCoprocessorSourceFactory.getInstance().getPhoenixTTLSource();
    }

    @Override
    protected boolean isRegionObserverFor(Scan scan) {
        return ScanUtil.isMaskTTLExpiredRows(scan) || ScanUtil.isDeleteTTLExpiredRows(scan);
    }

    @Override
    protected RegionScanner doPostScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c, final Scan scan,
                                              final RegionScanner s) throws IOException, SQLException {
        if (ScanUtil.isMaskTTLExpiredRows(scan) && ScanUtil.isDeleteTTLExpiredRows(scan)) {
            throw new IOException("Both mask and delete expired rows property cannot be set");
        } else if (ScanUtil.isMaskTTLExpiredRows(scan)) {
            metricSource.incrementMaskExpiredRequestCount();
            scan.setAttribute(PhoenixTTLRegionScanner.MASK_PHOENIX_TTL_EXPIRED_REQUEST_ID_ATTR,
                    Bytes.toBytes(String.format("MASK-EXPIRED-%d",
                            metricSource.getMaskExpiredRequestCount())));
        } else if (ScanUtil.isDeleteTTLExpiredRows(scan)) {
            metricSource.incrementDeleteExpiredRequestCount();
            scan.setAttribute(PhoenixTTLRegionScanner.MASK_PHOENIX_TTL_EXPIRED_REQUEST_ID_ATTR,
                    Bytes.toBytes(String.format("DELETE-EXPIRED-%d",
                            metricSource.getDeleteExpiredRequestCount())));
        }
        LOG.trace(String.format(
                "********** PHOENIX-TTL: PhoenixTTLRegionObserver::postScannerOpen TTL for table = "
                        + "[%s], scan = [%s], PHOENIX_TTL = %d ***************, "
                        + "numMaskExpiredRequestCount=%d, "
                        + "numDeleteExpiredRequestCount=%d",
                s.getRegionInfo().getTable().getNameAsString(),
                scan.toJSON(Integer.MAX_VALUE),
                ScanUtil.getPhoenixTTL(scan),
                metricSource.getMaskExpiredRequestCount(),
                metricSource.getDeleteExpiredRequestCount()
        ));
        return new PhoenixTTLRegionScanner(c.getEnvironment(), scan, s);
    }

    /**
     * A region scanner that checks the TTL expiration of rows
     */
    private static class PhoenixTTLRegionScanner extends BaseRegionScanner {
        private static final String MASK_PHOENIX_TTL_EXPIRED_REQUEST_ID_ATTR =
                "MASK_PHOENIX_TTL_EXPIRED_REQUEST_ID";

        private final RegionCoprocessorEnvironment env;
        private final RegionScanner scanner;
        private final Scan scan;
        private final byte[] emptyCF;
        private final byte[] emptyCQ;
        private final Region region;
        private final long minTimestamp;
        private final long maxTimestamp;
        private final long now;
        private final boolean deleteIfExpired;
        private final boolean maskIfExpired;
        private final String requestId;
        private final byte[] scanTableName;
        private long numRowsExpired;
        private long numRowsScanned;
        private long numRowsDeleted;
        private boolean reported = false;
        private long pageSizeMs;

        public PhoenixTTLRegionScanner(RegionCoprocessorEnvironment env, Scan scan,
                RegionScanner scanner) throws IOException {
            super(scanner);
            this.env = env;
            this.scan = scan;
            this.scanner = scanner;
            byte[] requestIdBytes = scan.getAttribute(MASK_PHOENIX_TTL_EXPIRED_REQUEST_ID_ATTR);
            this.requestId = Bytes.toString(requestIdBytes);

            deleteIfExpired = ScanUtil.isDeleteTTLExpiredRows(scan);
            maskIfExpired = !deleteIfExpired && ScanUtil.isMaskTTLExpiredRows(scan);

            region = env.getRegion();
            emptyCF = scan.getAttribute(EMPTY_COLUMN_FAMILY_NAME);
            emptyCQ = scan.getAttribute(EMPTY_COLUMN_QUALIFIER_NAME);
            scanTableName = scan.getAttribute(BaseScannerRegionObserver.PHOENIX_TTL_SCAN_TABLE_NAME);

            byte[] txnScn = scan.getAttribute(BaseScannerRegionObserver.TX_SCN);
            if (txnScn != null) {
                TimeRange timeRange = scan.getTimeRange();
                scan.setTimeRange(timeRange.getMin(), Bytes.toLong(txnScn));
            }
            minTimestamp = scan.getTimeRange().getMin();
            maxTimestamp = scan.getTimeRange().getMax();
            now = maxTimestamp != HConstants.LATEST_TIMESTAMP ?
                            maxTimestamp :
                            EnvironmentEdgeManager.currentTimeMillis();
            pageSizeMs = getPageSizeMsForRegionScanner(scan);
        }

        @Override public int getBatch() {
            return scanner.getBatch();
        }

        @Override public long getMaxResultSize() {
            return scanner.getMaxResultSize();
        }

        @Override public boolean next(List<Cell> result) throws IOException {
            return doNext(result, false);
        }

        @Override public boolean next(List<Cell> result, ScannerContext scannerContext)
                throws IOException {
            throw new IOException(
                    "next with scannerContext should not be called in Phoenix environment");
        }

        @Override public boolean nextRaw(List<Cell> result, ScannerContext scannerContext)
                throws IOException {
            throw new IOException(
                    "NextRaw with scannerContext should not be called in Phoenix environment");
        }

        @Override public void close() throws IOException {
            if (!reported) {
                LOG.debug(String.format(
                        "PHOENIX-TTL-SCAN-STATS-ON-CLOSE: " + "request-id:[%s,%s] = [%d, %d, %d]",
                        this.requestId, Bytes.toString(scanTableName),
                        this.numRowsScanned, this.numRowsExpired, this.numRowsDeleted));
                reported = true;
            }
            scanner.close();
        }

        @Override public RegionInfo getRegionInfo() {
            return scanner.getRegionInfo();
        }

        @Override public boolean reseek(byte[] row) throws IOException {
            return scanner.reseek(row);
        }

        @Override public long getMvccReadPoint() {
            return scanner.getMvccReadPoint();
        }

        @Override public boolean nextRaw(List<Cell> result) throws IOException {
            return doNext(result, true);
        }

        private boolean doNext(List<Cell> result, boolean raw) throws IOException {
            try {
                long startTime = EnvironmentEdgeManager.currentTimeMillis();
                boolean hasMore;
                do {
                    hasMore = raw ? scanner.nextRaw(result) : scanner.next(result);
                    if (result.isEmpty()) {
                        break;
                    }
                    if (isDummy(result)) {
                        return true;
                    }

                    /**
                     Note : That both MaskIfExpiredRequest and DeleteIfExpiredRequest cannot be set at the same time.
                     Case : MaskIfExpiredRequest, If row not expired then return.
                     */
                    numRowsScanned++;
                    if (maskIfExpired && checkRowNotExpired(result)) {
                        break;
                    }

                    /**
                     Case : DeleteIfExpiredRequest, If deleted then return.
                     So that it will count towards the aggregate deleted count.
                     */
                    if (deleteIfExpired && deleteRowIfExpired(result)) {
                        numRowsDeleted++;
                        break;
                    }
                    // skip this row
                    // 1. if the row has expired (checkRowNotExpired returned false)
                    // 2. if the row was not deleted (deleteRowIfExpired returned false and
                    //  do not want it to count towards the deleted count)
                    if (maskIfExpired) {
                        numRowsExpired++;
                    }
                    if (hasMore && (EnvironmentEdgeManager.currentTimeMillis() - startTime) >= pageSizeMs) {
                        byte[] rowKey = CellUtil.cloneRow(result.get(0));
                        result.clear();
                        getDummyResult(rowKey, result);
                        return true;
                    }
                    result.clear();
                } while (hasMore);
                return hasMore;
            } catch (Throwable t) {
                ServerUtil.throwIOException(region.getRegionInfo().getRegionNameAsString(), t);
                return false; // impossible
            }
        }

        /**
         * @param cellList is an input and output parameter and will either include a valid row or be an empty list
         * @return true if row expired and deleted or empty, otherwise false
         * @throws IOException
         */
        private boolean deleteRowIfExpired(List<Cell> cellList) throws IOException {

            long cellListSize = cellList.size();
            if (cellListSize == 0) {
                return true;
            }

            Iterator<Cell> cellIterator = cellList.iterator();
            Cell firstCell = cellIterator.next();
            byte[] rowKey = new byte[firstCell.getRowLength()];
            System.arraycopy(firstCell.getRowArray(), firstCell.getRowOffset(), rowKey, 0,
                    firstCell.getRowLength());

            boolean isRowExpired = !checkRowNotExpired(cellList);
            if (isRowExpired) {
                long ttl = ScanUtil.getPhoenixTTL(this.scan);
                long ts = ScanUtil.getMaxTimestamp(cellList);
                LOG.trace(String.format(
                        "PHOENIX-TTL: Deleting row = [%s] belonging to table = %s, "
                                + "scn = %s, now = %d, delete-ts = %d, max-ts = %d",
                        Bytes.toString(rowKey),
                        Bytes.toString(scanTableName),
                        maxTimestamp != HConstants.LATEST_TIMESTAMP,
                        now, now - ttl, ts));
                Delete del = new Delete(rowKey, now - ttl);
                Mutation[] mutations = new Mutation[] { del };
                region.batchMutate(mutations);
                return true;
            }
            return false;
        }

        /**
         * @param cellList is an input and output parameter and will either include a valid row
         *                 or be an empty list
         * @return true if row not expired, otherwise false
         * @throws IOException
         */
        private boolean checkRowNotExpired(List<Cell> cellList) throws IOException {
            long cellListSize = cellList.size();
            Cell cell = null;
            if (cellListSize == 0) {
                return true;
            }
            Iterator<Cell> cellIterator = cellList.iterator();
            while (cellIterator.hasNext()) {
                cell = cellIterator.next();
                if (ScanUtil.isEmptyColumn(cell, this.emptyCF, this.emptyCQ)) {
                    LOG.trace(String.format("** PHOENIX-TTL: Row expired for [%s], expired = %s **",
                            cell.toString(), ScanUtil.isTTLExpired(cell, this.scan, this.now)));
                    // Empty column is not supposed to be returned to the client
                    // except when it is the only column included in the scan.
                    if (cellListSize > 1) {
                        cellIterator.remove();
                    }
                    return !ScanUtil.isTTLExpired(cell, this.scan, this.now);
                }
            }
            LOG.warn("The empty column does not exist in a row in " + region.getRegionInfo()
                    .getTable().getNameAsString());
            return true;
        }

        @Override
        public RegionScanner getNewRegionScanner(Scan scan) throws IOException {
            return new PhoenixTTLRegionScanner(env, scan, ((BaseRegionScanner)delegate).getNewRegionScanner(scan));
        }
    }


    private static class PhoenixTTLStoreScanner extends StoreScanner {
        Scan viewScan;
        private long minTimestamp;
        private long maxTimestamp;
        private long now;
        private byte[] emptyCF;
        private byte[] emptyCQ;
        private Region region;

        public void setRegion(Region region) {
            this.region = region;
        }


        public PhoenixTTLStoreScanner(HStore store, ScanInfo scanInfo, Scan viewScan,
                                    List<? extends KeyValueScanner> scanners, ScanType scanType, long smallestReadPoint,
                                    long earliestPutTs) throws IOException {

            super(store, scanInfo, scanners, scanType, smallestReadPoint, earliestPutTs);
            this.viewScan = viewScan;
            emptyCF = viewScan.getAttribute(EMPTY_COLUMN_FAMILY_NAME);
            emptyCQ = viewScan.getAttribute(EMPTY_COLUMN_QUALIFIER_NAME);

            minTimestamp = viewScan.getTimeRange().getMin();
            maxTimestamp = viewScan.getTimeRange().getMax();
            now = maxTimestamp != HConstants.LATEST_TIMESTAMP ? maxTimestamp : EnvironmentEdgeManager.currentTimeMillis();
            LOG.info(String.format("***** 0.PHOENIX-TTL(Store): %s, %s ************",
                    store.getTableName().getNameAsString(), store.getColumnFamilyName()));

        }

        @Override public boolean next(List<Cell> outResult, ScannerContext scannerContext)
                throws IOException {

            LOG.info("0.PhoenixTTLStoreScanner scanner for PHOENIX-TTL. Stacktrace for informational purposes: "
                    +  LogUtil.getCallerStackTrace());

            LOG.info(String.format("***** 3.PHOENIX-TTL(Store): In next [%d] ************,", outResult.size()));
            //boolean hasMore = this.regionScanner.next(outResult, NoLimitScannerContext.getInstance());
            boolean hasMore = super.next(outResult, NoLimitScannerContext.getInstance());
            if (outResult != null && outResult.size() > 0 && checkEmptyColumnExpired(outResult)) {
                LOG.info(String.format("***** 7.PHOENIX-TTL(Store): after skipRowIfExpired ************"));
                outResult.clear();
            }
            return hasMore;
        }

        private boolean checkEmptyColumnExpired(List<Cell> cellList) throws IOException {
            LOG.info(String.format("***** 4.PHOENIX-TTL(Store): In skipRowIfExpired [%d] ************", cellList.size()));

            Iterator<Cell> cellIterator = cellList.iterator();
            Cell firstCell = cellIterator.next();
            byte[] rowKey = new byte[firstCell.getRowLength()];
            System.arraycopy(firstCell.getRowArray(), firstCell.getRowOffset(), rowKey, 0, firstCell.getRowLength());
            LOG.info(String.format("***** 5.PHOENIX-TTL(Store): Get Row row = %s ****** ", Bytes.toString(rowKey)));

            Get get = new Get(rowKey);
            get.setFilter(viewScan.getFilter());
            //get.setTimeRange(minTimestamp, maxTimestamp);
            //get.addColumn(emptyCF, emptyCQ);
            Result result = region.get(get);
            if (result.isEmpty()) {
                LOG.warn("6.PHOENIX-TTL(Store) The empty column does not exist in a row in " + region.getRegionInfo().getTable().getNameAsString());
                return false;
            }
            return ScanUtil.isTTLExpired(result.getColumnLatestCell(emptyCF, emptyCQ), this.viewScan, this.now);
        }


        private boolean skipRowIfExpired(List<Cell> cellList) throws IOException {

            LOG.info(String.format("***** 4.PHOENIX-TTL(Store): In skipRowIfExpired [%d] ************", cellList.size()));
            long cellListSize = cellList.size();
            if (cellListSize == 0) {
                return true;
            }

            Iterator<Cell> cellIterator = cellList.iterator();
            Cell firstCell = cellIterator.next();
            byte[] rowKey = new byte[firstCell.getRowLength()];
            System.arraycopy(firstCell.getRowArray(), firstCell.getRowOffset(), rowKey, 0, firstCell.getRowLength());
            long ttl = ScanUtil.getPhoenixTTL(this.viewScan) ;
            long ts = getMaxTimestampCell(cellList).getTimestamp();
            byte[] existingStartKey = viewScan.getStartRow();
            byte[] existingStopKey = viewScan.getStopRow();
            boolean isRowExpired = false;
            if ((Bytes.compareTo(existingStartKey, rowKey) < 0) && (Bytes.compareTo(rowKey, existingStopKey) < 0)) {
                if (ts + ttl < now) {
                    isRowExpired = true;
                }
                LOG.info(String.format("***** 5.PHOENIX-TTL(Store): Found Row = expired = %s", isRowExpired));
            }
            LOG.info(String.format("***** 6.PHOENIX-TTL(Store): Skipping Row row = %s, ttl = %d, "
                            + "delete-ts = %d, max-ts = %d, isRowExpired = %s ****** ",
                    Bytes.toString(rowKey),
                    ttl,
                    now-ttl,
                    ts,
                    isRowExpired));

            if (isRowExpired) {
                return true;
            }
            return false;
        }
        private Cell getMaxTimestampCell(List<Cell> cellList) {
            long maxTs = 0;
            long ts = 0;
            Cell maxTsCell = null;
            Iterator<Cell> cellIterator = cellList.iterator();
            while (cellIterator.hasNext()) {
                Cell cell = cellIterator.next();
                ts = cell.getTimestamp();
                if (ts > maxTs) {
                    maxTs = ts;
                    maxTsCell = cell;
                }
            }
            return maxTsCell;
        }
    }

    @Override
    public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store, List<? extends StoreFile> candidates, CompactionLifeCycleTracker tracker) throws IOException {
        super.preCompactSelection(c, store, candidates, tracker);
    }

    @Override
    public void postCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store, List<? extends StoreFile> selected, CompactionLifeCycleTracker tracker, CompactionRequest request) {
        super.postCompactSelection(c, store, selected, tracker, request);

        if (tracker != null && tracker.getClass().isAssignableFrom(
                PhoenixTTLCompactorEndpoint.PhoenixTTLCompactionTracker.class)) {

            Scan scan = ((PhoenixTTLCompactorEndpoint.PhoenixTTLCompactionTracker) tracker).getScan();
            String tenantId = Bytes.toString(scan.getAttribute("__TenantId"));
            String viewName = Bytes.toString(scan.getAttribute("__ViewName"));
            String startRow = Bytes.toString(scan.getStartRow());
            String stopRow = Bytes.toString(scan.getStopRow());
            String filter = scan.getFilter() != null ? scan.getFilter().toString() : "NO_FILTER";

            LOG.info(String.format(
                    "********** 1.PHOENIX-TTL(Store): PhoenixTTLStoreScanner::postCompactSelection "
                            + "region = [%s], "
                            + "tenantId = %s  "
                            + "viewName = %s  "
                            + "startRow = %s  "
                            + "stopRow = %s  "
                            + "filter = %s  "
                            + "***************",
                    c.getEnvironment().getRegionInfo().getRegionNameAsString(),
                    tenantId,
                    viewName,
                    startRow,
                    stopRow,
                    filter));

        }

    }

    @Override
    public void preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store, ScanType scanType, ScanOptions options, CompactionLifeCycleTracker tracker, CompactionRequest request) throws IOException {
        super.preCompactScannerOpen(c, store, scanType, options, tracker, request);
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store, InternalScanner scanner, ScanType scanType, CompactionLifeCycleTracker tracker, CompactionRequest request) throws IOException {
        if (tracker != null && tracker.getClass().isAssignableFrom(
                PhoenixTTLCompactorEndpoint.PhoenixTTLCompactionTracker.class)) {
            //c.bypass();
            //c.complete();
            Configuration conf = c.getEnvironment().getConfiguration();
            HStore thisStore = ((HStore)store);



            Scan scan = ((PhoenixTTLCompactorEndpoint.PhoenixTTLCompactionTracker) tracker).getScan();
            String tenantId = Bytes.toString(scan.getAttribute("__TenantId"));
            String viewName = Bytes.toString(scan.getAttribute("__ViewName"));
            String startRow = Bytes.toString(scan.getStartRow());
            String stopRow = Bytes.toString(scan.getStopRow());
            String filter = scan.getFilter() != null ? scan.getFilter().toString() : "NO_FILTER";

            LOG.info(String.format(
                    "********** 2.PHOENIX-TTL(Store): PhoenixTTLStoreScanner::preCompact "
                            + "region = [%s], "
                            + "tenantId = %s  "
                            + "viewName = %s  "
                            + "startRow = %s  "
                            + "stopRow = %s  "
                            + "filter = %s  "
                            + "***************",
                    c.getEnvironment().getRegionInfo().getRegionNameAsString(),
                    tenantId,
                    viewName,
                    startRow,
                    stopRow,
                    filter));


            Region region = c.getEnvironment().getRegion();
//            RegionScanner scanner = region.getScanner(scan);
            PhoenixTTLStoreScanner storeScanner =  new PhoenixTTLRegionObserver.PhoenixTTLStoreScanner(thisStore, thisStore.getScanInfo(),
                    scan,
                    Lists.newArrayList(),
                    scanType, store.getSmallestReadPoint(), HConstants.NO_NONCE);
            storeScanner.setRegion(region);
            return storeScanner;

        }
        return super.preCompact(c, store, scanner, scanType, tracker, request);
    }
}
