package org.apache.phoenix.end2end;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.util.ScanUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class SampleTestIT extends LocalHBaseIT {
    @Test public void test6123() throws Exception {
        try (Connection globalConn = DriverManager.getConnection(getUrl());
                final Statement statement = globalConn.createStatement()) {
            PhoenixConnection phxConn = globalConn.unwrap(PhoenixConnection.class);
            MetaDataClient client = new MetaDataClient(phxConn);
            MetaDataProtocol.MetaDataMutationResult result = client.updateCache("V1", true);

            final String stmtString = String.format("select * from  %s", "V1");
            Preconditions.checkNotNull(stmtString);
            final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);
            final QueryPlan queryPlan = pstmt.optimizeQuery(stmtString);

            PhoenixResultSet rs = pstmt.newResultSet(queryPlan.iterator(), queryPlan.getProjector(),
                    queryPlan.getContext());
            Assert.assertFalse("Should not have any rows", rs.next());
            Assert.assertEquals("Should have atleast one element", 1, queryPlan.getScans().size());
            Scan scan = queryPlan.getScans().get(0).get(0);
            int cv = ScanUtil.getClientVersion(scan);
            Assert.assertEquals("Expected version", VersionUtil.encodeVersion(4, 16, 0), cv);

        }
    }
/*
    @Test public void testPMFs() throws Exception {
        List<Pair<String, Double>> pmf = Lists.newArrayList();
        pmf.add(new Pair("a", 0.1));
        pmf.add(new Pair("b", 0.1));
        pmf.add(new Pair("c", 0.1));
        pmf.add(new Pair("d", 0.1));
        pmf.add(new Pair("e", 0.1));
        pmf.add(new Pair("f", 1.4));
        pmf.add(new Pair("g", 0.1));

        EnumeratedDistribution<String> distribution = new EnumeratedDistribution(pmf);
        Map<String, Integer> results = Maps.newHashMap();
        for (int i = 0; i < 1; i++) {
            String sample = distribution.sample();
            System.out.println(String.format("sample = %s, occ = %d", sample, results.get(sample)));

            Integer counts = results.get(sample);
            if (counts == null) {
                counts = new Integer(0);
            }
            counts++;
            results.put(sample, counts);
        }

        for (String key : results.keySet()) {
            System.out.println(String.format("sample = %s, occ = %d", key, results.get(key)));
        }

    }
    @Ignore public void testPMFs() throws Exception {
        List<Pair<String, Double>> pmf = Lists.newArrayList();
        pmf.add(new Pair("f", 0.4));
        pmf.add(new Pair("null", 0.6));

        EnumeratedDistribution<String> distribution = new EnumeratedDistribution(pmf);
        Map<String, Integer> results = Maps.newHashMap();
        for (int i = 0; i < 2000; i++) {
            String sample = distribution.sample();
            //System.out.println(String.format("sample = %s, occ = %d", sample, results.get(sample)));

            Integer counts = results.get(sample);
            if (counts == null) {
                counts = new Integer(0);
            }
            counts++;
            results.put(sample, counts);
        }

        for (String key : results.keySet()) {
            System.out.println(String.format("sample = %s, occ = %d", key, results.get(key)));
        }

    }



 */
}
