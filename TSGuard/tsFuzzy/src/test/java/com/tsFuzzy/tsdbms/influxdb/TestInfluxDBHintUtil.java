package com.tsFuzzy.tsdbms.influxdb;

import com.benchmark.dto.DBValParam;
import com.benchmark.entity.DBVal;
import com.benchmark.entity.PerformanceEntity;
import com.benchmark.constants.DataType;
import com.benchmark.constants.ValueStatus;
import com.benchmark.influxdb.hint.FluxHintInjector;
import com.benchmark.influxdb.db.InfluxdbDBApiEntry;
import com.benchmark.influxdb.influxdbUtil.ParseData;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxTable;


public class TestInfluxDBHintUtil {

    private static final String HOST      = "localhost";
    private static final int    PORT      = 8086;
    private static final String TOKEN     = "yQmxE44rWrGffiK_O9xgTiVz-__nXmkoS0zsrAZ3aEVup9jHu_tRaJO_aJpam9DQp99NAE8UoSgS9H66fFxCiQ==";  // ← replace with a real token
    private static final String ORG       = "1db4a79f8d1dc77c";    // ← replace with your org
    private static final String PRECISION = "ms";
    private static final String BUCKET = "test_bucket";

    private static InfluxdbDBApiEntry entry;

    private DBVal makeVal(String tagValue, double fieldValue, long timestampMs) {
        return DBVal.DBValBuilder.anDBVal()
                .withTableName("measurement1")
                .withTagName("tag1")
                .withTagValue(tagValue)
                .withUtcTime(timestampMs)
                .withValueStatus(ValueStatus.VALID)
                .withFieldNames(Arrays.asList("value"))
                .withFieldTypes(Arrays.asList(DataType.DOUBLE))
                .withFieldValues(Arrays.asList(fieldValue))
                .withFieldSize(1)
                .build();
    }

    /** helper to flatten FluxTable → Hashtable → List<DBVal> */
    private List<DBVal> fluxToDbVals(List<FluxTable> tables, String tagName) {
        Hashtable<String, SortedMap<Long, DBVal>> parsed =
                ParseData.parseFluxTableToDBVal(tables, tagName);
        return parsed.values()
                .stream()
                .flatMap(m -> m.values().stream())
                .collect(Collectors.toList());
    }

    //delete
    @Test
    public void deleteHintBucket() throws IOException {

        entry = new InfluxdbDBApiEntry(HOST, PORT, TOKEN, ORG, "", PRECISION);


        for (int i = 0; i < 100; i++) {
            String bucket = "hintdb" + i;
            entry.deleteBucket(bucket);
        }

    }

    @Test
    public void e2eSlimitSoffsetHonored() {
        // 1) insert 4 points with increasing field values 1,2,3,4
        List<DBVal> pts = new ArrayList<>();
        for (int i = 1; i <= 4; i++) {
            pts.add(DBVal.DBValBuilder.anDBVal()
                    .withTableName("m")
                    .withTagName("t")
                    .withTagValue("x")
                    .withUtcTime(i*1000)
                    .withValueStatus(ValueStatus.VALID)
                    .withFieldNames(Arrays.asList("f"))
                    .withFieldTypes(Arrays.asList(DataType.DOUBLE))
                    .withFieldValues(Arrays.asList((double)i))
                    .withFieldSize(1)
                    .build());
        }
        assertTrue(entry.sendMultiPoint(pts).isSuccess());

        // 1) query *without* hint
        String baseFlux =
                "from(bucket:\"" + BUCKET + "\")" +
                        " |> range(start:0,stop:10)" +
                        " |> filter(fn: r => r._measurement == \"m\")";
        List<DBVal> all =
                fluxToDbVals(
                        entry.getInfluxDBClient()
                                .getQueryApi()
                                .query(baseFlux),
                        "t"  // your tagName
                );
        assertEquals(4, all.size());

        // 2) inject SLIMIT 2 SOFFSET 1
        String limitedFlux = FluxHintInjector.applyHint(baseFlux, "SLIMIT 2 SOFFSET 1");
        List<DBVal> limited =
                fluxToDbVals(
                        entry.getInfluxDBClient()
                                .getQueryApi()
                                .query(limitedFlux),
                        "t"
                );
        assertEquals(2, limited.size());
        assertEquals(2.0, limited.get(0).getFieldValues().get(0));
        assertEquals(3.0, limited.get(1).getFieldValues().get(0));
    }

    @Test
    public void testCreateBucketAndInsertData() throws Exception {

        // 2) ensure a clean bucket each run
        assertTrue(entry.deleteBucket(BUCKET), "should delete old bucket without error");
        assertTrue(entry.createBucket(BUCKET, 24 * 60 * 60 * 1000L),
                "should create new bucket successfully");

        // 3) prepare three points at now -1s, now, now +1s
        long now = System.currentTimeMillis();
        List<DBVal> toWrite = Arrays.asList(
                makeVal("A", 1.1, now - 1_000),
                makeVal("B", 2.2, now),
                makeVal("C", 3.3, now + 1_000)
        );

        // 4) write them
        PerformanceEntity perf = entry.sendMultiPoint(toWrite);
        assertTrue(perf.isSuccess(), "data write should succeed");

        // 5) read them back via Flux
        QueryApi queryApi = entry.getInfluxDBClient().getQueryApi();
        // note: Influx range() takes seconds, so divide by 1000
        String flux = String.format(
                "from(bucket:\"%s\") |> range(start:%d, stop:%d)",
                BUCKET,
                (now - 2_000) / 1000,
                (now + 2_000) / 1000
        );
        List<FluxTable> tables = queryApi.query(flux);

        // parse into a single list
        Map<String, SortedMap<Long,DBVal>> byTag = ParseData.parseFluxTableToDBVal(tables, "tag1");
        List<DBVal> readBack = new ArrayList<>();
        byTag.values().forEach(tm -> readBack.addAll(tm.values()));

        // 6) verify count and basic round-trip
        assertEquals(toWrite.size(), readBack.size(),
                "should read back exactly the same number of points");
    }

    @Test
    public void testInsertAndQuery() throws Exception {
        // 1) connect & reset bucket
        InfluxdbDBApiEntry api = new InfluxdbDBApiEntry(HOST, PORT, TOKEN, ORG, BUCKET, "ms");
        assertTrue(api.deleteBucket(BUCKET), "delete old bucket");
        assertTrue(api.createBucket(BUCKET, 24 * 60 * 60 * 1000L), "should recreate bucket");

        // 2) first batch
        long now = System.currentTimeMillis();
        List<DBVal> batch1 = Arrays.asList(
                makeVal("A", 1.1, now - 2_000),
                makeVal("B", 2.2, now - 1_000)
        );
        assertTrue(api.sendMultiPoint(batch1).isSuccess(), "batch1 write");

        // 3) second batch
        List<DBVal> batch2 = Arrays.asList(
                makeVal("C", 3.3, now),
                makeVal("D", 4.4, now + 1_000)
        );
        assertTrue(api.sendMultiPoint(batch2).isSuccess(), "batch2 write");

        // 4) plain Flux query for “last hour”
        QueryApi queryApi = api.getInfluxDBClient().getQueryApi();
        String flux = String.format(
                "from(bucket:\"%s\") |> range(start: -1h)\n",
                BUCKET
        );
        List<FluxTable> tables = queryApi.query(flux);

        // 5) parse + flatten
        Map<String, SortedMap<Long,DBVal>> grouped = ParseData.parseFluxTableToDBVal(tables, "tag1");
        List<DBVal> all = new ArrayList<>();
        grouped.values().forEach(m -> all.addAll(m.values()));

        // 6) display and assert
        System.out.println("---- Result ----");
        all.forEach(System.out::println);
        assertEquals(3, all.size(), "should have read back 3 points");
    }

    @Test
    public void testHintInjection() {
        // 1. Define query parameters
        long start = System.currentTimeMillis() - 10_000;
        long end = System.currentTimeMillis();
        DBValParam param = DBValParam.builder()
                .tableName("m")
                .tagName("t")
                .tagValue("x")
                .valueStatus(ValueStatus.VALID)
                .build();

        // 2. Generate the base Flux query (without hints)
        String baseFlux = String.format(
                "from(bucket:\"%s\")\n  |> range(start: %d, stop: %d)\n  |> filter(fn: (r) => r._measurement == \"%s\" and r.%s == \"%s\")",
                BUCKET, start / 1000, end / 1000, param.getTableName(), param.getTagName(), param.getTagValue()
        );

        String fluxWithHint = FluxHintInjector.applyHint(baseFlux, "SLIMIT 2 SOFFSET 1");


        System.out.println("Final Flux Query:\n" + fluxWithHint);
    }








}
