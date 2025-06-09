package com.benchmark.influxdb.db;

import com.benchmark.commonClass.ApiEntry;
import com.benchmark.commonUtil.CommonUtil;
import com.benchmark.commonUtil.SpentTimeCalculator;
import com.benchmark.constants.AggFunctionType;
import com.benchmark.constants.ValueStatus;
import com.benchmark.dto.DBValParam;
import com.benchmark.entity.AggCountResult;
import com.benchmark.entity.DBVal;
import com.benchmark.entity.PerformanceEntity;
import com.benchmark.influxdb.hint.ExpectedResultGenerator;
import com.benchmark.influxdb.hint.ResultComparator;
import com.benchmark.influxdb.influxdbUtil.ParseData;
import com.influxdb.client.*;
import com.influxdb.client.domain.Bucket;
import com.influxdb.client.domain.BucketRetentionRules;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.query.FluxTable;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.benchmark.influxdb.hint.FluxHintInjector;

@Slf4j
@Data
public class InfluxdbDBApiEntry implements ApiEntry {
    private String host;
    private int port;
    private char[] token;
    private String orgId;
    private String bucket;
    private String precision;       // 时间戳精度
    InfluxDBClient influxDBClient;
    private final static String measurementName = "driverLocation";
    private long lookback = 1000;                        // 1s内为实时查询
    private long lastLookback = 1000 * 60 * 60 * 24 * 31L;       // 1个月内数据均为最近点查询
    private int timeout = 60; //seconds
    private int splitSize = 200000;
    //influx offset - 表示计算时的偏移量，取整点。例如1h表示从当前时间所在的小时整点开始计算。
    private String offset = "-8h";
    private long increaseLookBack = 24 * 60 * 60 * 1000; //单位毫秒
    private int slowQueryTimeThrehold = 20000; //单位毫秒
    private boolean trimTagValue = true;
    private int splitSizeRealTime = 200;       // 分片大小
    private String hint = "";

    /**
     * 初始化DBApiEntry。
     *
     * @param host        主机ip，如"127.0.0.1"。
     * @param port        端口，如 8086。
     * @param tokenString influxDB的 api token。
     * @param orgId       Organization Id, 8长度字符串。
     * @param bucket      对应插入数据的bucket名称。
     */
    public InfluxdbDBApiEntry(String host, int port, String tokenString,
                              String orgId, String bucket, String precision) throws IOException {
        this.host = host;
        this.port = port;
        this.token = tokenString.toCharArray();
        this.orgId = URLEncoder.encode(orgId, "utf-8");
        this.bucket = URLEncoder.encode(bucket, "utf-8");
        this.precision = URLEncoder.encode(precision, "utf-8");
        String url = String.format("http://%s:%d", this.host, this.port);
        log.info(String.format("create InfluxdbDBApiEntry url:%s orgId:%s bucket:%s", url, orgId, bucket));
//        this.influxDBClient = InfluxDBClientFactory.create(url, this.token, this.orgId, this.bucket);

        OkHttpClient.Builder builder = new OkHttpClient.Builder()
                .readTimeout(this.timeout, TimeUnit.SECONDS)
                .writeTimeout(this.timeout, TimeUnit.SECONDS)
                .connectTimeout(this.timeout, TimeUnit.SECONDS);
        InfluxDBClientOptions options = InfluxDBClientOptions.builder()
                .url(url)
                .authenticateToken(this.token)
                .org(this.orgId)
                .bucket(this.bucket)
                .precision(WritePrecision.fromValue(this.precision))
                .okHttpClient(builder)
                .build();
        this.influxDBClient = InfluxDBClientFactory.create(options);
    }

    /**
     * 释放influxclient资源
     */
    public void close() {
        this.influxDBClient.close();
        log.info(String.format("close InfluxdbDBApiEntry host:%s port:%d org:%s bucket:%s", host, port,
                orgId, bucket));
    }

    public void setHint(String hint) {
        this.hint = hint == null ? "" : hint.trim();
    }

    private List<FluxTable> runFluxWithHint(String baseFlux, String hint) {
        String fluxWithHint = FluxHintInjector.applyHint(baseFlux, hint);
        return influxDBClient.getQueryApi().query(fluxWithHint);
    }

    //for testing only
    public boolean createBucket(String name, long retentionPeriodMs) {
        try {
            BucketRetentionRules rule = new BucketRetentionRules();
            // API takes seconds
            int everySeconds = Math.max(1, (int)(retentionPeriodMs / 1000));
            rule.setEverySeconds(everySeconds);

            BucketsApi bucketsApi       = influxDBClient.getBucketsApi();
            // find your org by ID
            Bucket bucket = bucketsApi.createBucket(name, rule, this.orgId);
            return bucket != null;
        } catch (Exception e) {
            log.warn("createBucket failed", e);
            return false;
        }
    }

    public boolean deleteBucket(String name) {
        try {
            BucketsApi bucketsApi = influxDBClient.getBucketsApi();
            Bucket bucket = bucketsApi.findBucketByName(name);
            if (bucket == null) {
                return true;
            }
            bucketsApi.deleteBucket(bucket);
            return true;
        } catch (Exception e) {
            log.warn("deleteBucket failed", e);
            return false;
        }
    }



    @Override
    public boolean isConnected() {
        return this.influxDBClient.ping();
    }

    @Override
    public boolean disConnect() {
        if (isConnected()) this.close();
        return true;
    }

    private void logSlowQuery(String method, String query, long spendTime) {
        if (spendTime > this.slowQueryTimeThrehold) {
            log.error(String.format("slowQuery spendTime:%d(ms) method:%s query:%s",
                    spendTime, method, query));
        }
    }

    /**
     * 是否进行 trim 操作
     *
     * @return
     */
    public boolean isTrimTagValue() {
        return trimTagValue;
    }

    /**
     * 点名合法性检查函数，强化版
     *
     * @return
     * @date 2020-11-23
     */
    protected String trimStrong(String tagName) {
        try {
            String regEx = "[\n`~!@#$%^&*(){}':;',\\[\\].<>/?~！@#￥%……&*（）+{}【】‘；：”“’。， 、？] \\s";
            tagName = tagName.replaceAll(regEx, "");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tagName;
    }

    @Override
    public PerformanceEntity sendMultiPoint(List<DBVal> dbVals) {
        return this.sendPointDatas(dbVals);
    }

    /**
     * @param dbVals
     * @return int
     * @description 以DBVal格式发送若干数据
     * @dateTime 2023/2/4 17:21
     */
    protected PerformanceEntity sendPointDatas(List<DBVal> dbVals) {
        long timeCost = 0;
        long totalSize = 0;
        int counts = 0;
        StringBuilder sentence = new StringBuilder(50000);
        String item = "";
        for (DBVal val : dbVals) {
            item = ParseData.convertToLineProtocol(val);
            sentence.append(item).append("\n");

            counts++;
            if (counts > splitSize) {
                totalSize += counts;
                // 内部计时
                SpentTimeCalculator stc = SpentTimeCalculator.create().begin();
                if (!sendPointDataToDB(sentence.toString()))
                    return PerformanceEntity.builder()
                            .isSuccess(false)
                            .build();
                stc.end();
                timeCost += stc.getSpendTime();

                counts = 0;
                sentence.setLength(0);
            }
        }
        //将不够长的一起发出去
        if (sentence.length() != 0) {
            // 内部计时
            SpentTimeCalculator stc = SpentTimeCalculator.create().begin();
            if (!sendPointDataToDB(sentence.toString()))
                return PerformanceEntity.builder()
                        .isSuccess(false)
                        .build();
            stc.end();
            timeCost += stc.getSpendTime();
            totalSize += counts;
        }

        return PerformanceEntity.builder()
                .timeCost(timeCost)
                .obj(totalSize)
                .isSuccess(true)
                .build();
    }

    /**
     * @param sentences 行协议
     * @return void
     * @description 通过建立url连接，以行协议形式批量发送数据，sendPointDatas调用
     * @dateTime 2023/2/4 16:16
     */
    protected boolean sendPointDataToDB(String sentences) {
        String sendDataUrl = "http://" + this.host + ":" + this.port + "/api/v2/write?"
                + "org=" + this.orgId + "&bucket=" + this.bucket + "&precision=" + this.precision;
        try {
            URLConnection conn = new URL(sendDataUrl).openConnection();
            conn.setRequestProperty("accept", "application/json");
            conn.setRequestProperty("Content-Type", "text/plain; charset=utf-8");
            conn.setRequestProperty("Authorization", "Token " + String.valueOf(this.token));
            conn.setDoOutput(true);
            conn.setDoInput(true);
            // fill and send content
            DataOutputStream dos = new DataOutputStream(conn.getOutputStream());
            dos.write(sentences.getBytes());
            dos.flush();
            // get response (Do not comment this line, or the data insertion will be failed)
            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            // String line;
            // String result = "";
            // while ((line = in.readLine()) != null) {
            //     result += line;
            // }
            // System.out.println(result);
            return true;
        } catch (Exception e) {
            log.warn(String.format("插入数据异常: %s", e));
            return false;
        }
    }

    //TODO 以下若干抽象类均未实现
    @Override
    public int sendPointData(String tagName, List<DBVal> dbValList) {
        return 0;
    }

    @Override
    public int sendSinglePoint(String tagName, List<DBVal> dbValList) {
        return 0;
    }

    @Override
    public int updatePointValues(String tag, String startTime, String endTime, List<DBVal> updateDatas) throws ParseException {
        return 0;
    }

    @Override
    public boolean deletePointData(String tagName) {
        return false;
    }

    // TODO 将传参DBVal改为前端传入的数据
    @Override
    public DBVal getRTValue(DBValParam dbVal) {
        DBVal result;

        // 判断传参字符串是否为空
        if (StringUtils.isBlank(dbVal.getTagName()) || StringUtils.isBlank(dbVal.getTagValue())) {
            result = DBVal.DBValBuilder.anDBVal().build();
            result.setValueStatusInvalid();
            return result;
        }
        result = getDataAtUtcDBVal(System.currentTimeMillis() - lookback, System.currentTimeMillis(), dbVal);
        if (result == null) {
            result = DBVal.DBValBuilder.anDBVal().build();
            result.setValueStatusInvalid();
        }
        return result;
    }

    @Override
    public DBVal getLastValue(DBValParam dbValParam) {
        return null;
    }

    // 查询最近点数据 - 目前lastLookback表明1月内数据
    @Override
    public List<DBVal> getLastValueList(List<DBValParam> dbValParamList) {
        List<DBVal> results = new ArrayList<>();
        // 将dbValParamList依据tableName 和 tagName分组
        Hashtable<Pair<String, String>, List<DBValParam>> hashtable = CommonUtil.groupDBValParamByTableNameAndTagName(dbValParamList);
        for (Pair<String, String> pair : hashtable.keySet()) {
            // TODO 由于解析结果包含tableName，此处是否仅需按tagName分组？
            String tableName = pair.getKey();
            String tagName = pair.getValue();
            List<DBValParam> dbValParams = hashtable.get(pair);

            List<DBVal> dbVals = getDataListAtTimeRange(System.currentTimeMillis() - lastLookback,
                    System.currentTimeMillis(), tableName, tagName, dbValParams, true);

            if (!dbVals.isEmpty()) {
                // 按照传参的tagValues进行排序(原结果集按照fieldName排序)。需用LinkedHashSet才能保证添加顺序，而非HashSet(乱序)
                // 若获取全部对象，则不筛选
                Set<String> tagValues = new LinkedHashSet<>();
                for (DBValParam dbValParam : dbValParams) {
                    if (dbValParam.getTagValue().isEmpty() || dbValParam.getTagValue().equals("*")) {
                        tagValues.clear();
                        break;
                    } else {
                        tagValues.add(dbValParam.getTagValue());
                    }
                }

                results.addAll(turnListOrdered(tagValues, dbVals));
            }
        }
        return results;
    }

    /**
     * data: 2020-7-19 抽取所有数据获取的通用方法 不支持模糊查询
     * getRTValueList调用
     */
    protected DBVal getDataAtUtcDBVal(long beginDate, long endDate, DBValParam dbValParam) {
        DBVal dbVal = DBVal.DBValBuilder.anDBVal()
                .withValueStatus(ValueStatus.INVALID)
                .build();
        if (dbValParam == null) {
            log.warn(String.format("dbValParam is null"));
            return dbVal;
        }

        SpentTimeCalculator stc = SpentTimeCalculator.create().begin();

        String fluxQuery = String.format("from(bucket:\"%s\")  |> range(start: %d, stop: %d) " +
                        "|> filter(fn: (r) => (r._measurement == \"%s\" and r.%s == \"%s\")) |> last() ",
                this.bucket, beginDate / 1000, endDate / 1000, dbValParam.getTableName(), dbValParam.getTagName(),
                dbValParam.getTagValue());

        //QueryApi queryApi = influxDBClient.getQueryApi();
        //List<FluxTable> tables = queryApi.query(fluxQuery);
        List<FluxTable> tables = runFluxWithHint(fluxQuery, this.hint);

        Hashtable<String, SortedMap<Long, DBVal>> dbValHashtable = ParseData.parseFluxTableToDBVal(tables,
                dbValParam.getTagName());
        stc.end();
        log.info(String.format("queryLast tagName:%s tagValue:%s beginDate:%s endDate:%s spendTime:%d(ms)",
                dbValParam.getTagName(), dbValParam.getTagValue(),
                CommonUtil.uTCMilliSecondsToDateStringWithMs(beginDate),
                CommonUtil.uTCMilliSecondsToDateString(endDate), stc.getSpendTime()));
        logSlowQuery("queryLast", fluxQuery, stc.getSpendTime());

        if (dbValHashtable.containsKey(dbValParam.getTagValue())) {
            SortedMap timeSery = dbValHashtable.get(dbValParam.getTagValue());
            if (!timeSery.isEmpty())
                dbVal = (DBVal) timeSery.values().iterator().next();
        }

        return dbVal;
    }

    // 500ms内均作为实时值
    @Override
    public List<DBVal> getRTValueList(@NonNull List<DBValParam> dbValParamList) {
        List<DBVal> results = new ArrayList<>();
        // 将dbValParamList依据tableName 和 tagName分组
        Hashtable<Pair<String, String>, List<DBValParam>> hashtable = CommonUtil.groupDBValParamByTableNameAndTagName(dbValParamList);
        for (Pair<String, String> pair : hashtable.keySet()) {
            // TODO 由于解析结果包含tableName，此处是否仅需按tagName分组？
            String tableName = pair.getKey();
            String tagName = pair.getValue();
            List<DBValParam> dbValParams = hashtable.get(pair);

            List<DBVal> dbVals = getDataListAtTimeRange(System.currentTimeMillis() - lookback,
                    System.currentTimeMillis(), tableName, tagName, dbValParams, true);

            if (!dbVals.isEmpty()) {
                // 按照传参的tagValues进行排序(原结果集按照fieldName排序)。需用LinkedHashSet才能保证添加顺序，而非HashSet(乱序)
                // 若获取全部对象，则不筛选
                Set<String> tagValues = new LinkedHashSet<>();
                for (DBValParam dbValParam : dbValParams) {
                    if (dbValParam.getTagValue().isEmpty() || dbValParam.getTagValue().equals("*")) {
                        tagValues.clear();
                        break;
                    } else {
                        tagValues.add(dbValParam.getTagValue());
                    }
                }

                results.addAll(turnListOrdered(tagValues, dbVals));
            }
        }
        return results;
    }

    /**
     * @param beginDate
     * @param endDate
     * @param tagName
     * @param dbValParamList
     * @return java.util.List<db.DBVal>
     * @description 获取同一tableName, tagName，多个tagValue下数据。由getRTValueList调用
     * @dateTime 2023/2/15 13:50
     */
    protected List<DBVal> getDataListAtTimeRange(long beginDate, long endDate, String tableName,
                                                 String tagName, List<DBValParam> dbValParamList, boolean isLast) {
        List<DBVal> results = new ArrayList<>();
        if (dbValParamList == null) {
            log.warn(String.format("dbValParamList is null!"));
            return results;
        }
        if (StringUtils.isBlank(tableName) || StringUtils.isBlank(tagName)) {
            log.warn(String.format("tableName or tagName is empty!"));
            return results;
        }
        if (beginDate > endDate) {
            log.info(String.format("The query time range is empty!"));
            return results;
        } else if (beginDate == endDate) endDate += 1000;        // 左闭右开 + 1s，查询粒度为s

        // 解析DBValParam为筛选语句
        String sentence = ParseData.dbValParamsToFilterString(tagName, dbValParamList);

        if (StringUtils.isBlank(sentence)) {
            log.info(String.format("tagValues is empty!"));
            return results;
        }

        SpentTimeCalculator stc = SpentTimeCalculator.create().begin();
        String fluxQuery;
        if (isLast) {
            fluxQuery = String.format("from(bucket:\"%s\")  |> range(start: %d, stop: %d) " +
                            "|> filter(fn: (r) => (r._measurement == \"%s\" and (%s))) |> last() ",
                    this.bucket, beginDate / 1000, endDate / 1000, tableName, sentence);
        } else {
            fluxQuery = String.format("from(bucket:\"%s\")  |> range(start: %d, stop: %d) " +
                            "|> filter(fn: (r) => (r._measurement == \"%s\" and (%s)))",
                    this.bucket, beginDate / 1000, endDate / 1000, tableName, sentence);
        }

        QueryApi queryApi = influxDBClient.getQueryApi();
        List<FluxTable> rawTables = queryApi.query(fluxQuery);
        List<DBVal> baseline = ParseData
                .parseFluxTableToDBVal(rawTables, tagName)
                .values().stream()
                .flatMap(m -> m.values().stream())
                .collect(Collectors.toList());

        List<FluxTable> tables = runFluxWithHint(fluxQuery, this.hint);

        Hashtable<String, SortedMap<Long, DBVal>> dbValHashtable = ParseData.parseFluxTableToDBVal(tables,tagName);
        List<DBVal> actual = ParseData
                .parseFluxTableToDBVal(tables, tagName)
                .values().stream()
                .flatMap(m -> m.values().stream())
                .collect(Collectors.toList());

        //expected result to compare with actual
        List<DBVal> expected = ExpectedResultGenerator.apply(baseline, this.hint);
        List<String> mismatches = ResultComparator.compare(expected, actual);
        if (!mismatches.isEmpty()) {
            log.error("Hint `{}` produced unexpected results:", this.hint);
            mismatches.forEach(msg -> log.error("  • " + msg));
        }

        stc.end();
        log.info(String.format("queryLast tableName:%s tagName:%s beginDate:%s endDate:%s " +
                        "spendTime:%d(ms)",
                tableName, tagName,
                CommonUtil.uTCMilliSecondsToDateStringWithMs(beginDate),
                CommonUtil.uTCMilliSecondsToDateString(endDate), stc.getSpendTime()));
        logSlowQuery("queryLast", fluxQuery, stc.getSpendTime());

        if (dbValHashtable.isEmpty()) return results;

        // 将dbValHashtable转为dbValList
        dbValHashtable.values().forEach(timeSery -> {
            timeSery.values().forEach(dbVal -> results.add((DBVal) dbVal));
        });
        return results;
    }

    /**
     * 使List的内元素的顺序按照TagValues的顺序，若tagValues为空，则直接返回
     *
     * @date 2020-11-23
     */
    protected List<DBVal> turnListOrdered(Set<String> tagValues, List<DBVal> dbValList) {
        if (tagValues.isEmpty()) {
            return dbValList;
        }

        Map<String, List<DBVal>> map = new HashMap<>();
        for (DBVal dbVal : dbValList) {
            if (map.containsKey(dbVal.getTagValue())) {
                map.get(dbVal.getTagValue()).add(dbVal);
            } else {
                List<DBVal> tempList = new ArrayList<>();
                tempList.add(dbVal);
                map.put(dbVal.getTagValue(), tempList);
            }
        }

        List<DBVal> newDbvals = new ArrayList<>();
        for (String tagValue : tagValues) {
            if (map.containsKey(tagValue)) {
                newDbvals.addAll(map.get(tagValue));
            } else {
                log.info(String.format("result is empty: tagValue=%s", tagValue));
            }
        }
        return newDbvals;
    }

    @Override
    public List<DBVal> getRTValueListUseSplit(@NonNull List<DBValParam> dbValParamList) {
        List<DBVal> results = new ArrayList<>();
        if (dbValParamList.isEmpty()) {
            log.warn(String.format("dbValParams is empty!"));
            return results;
        }

        // 将dbValParamList依据tableName 和 tagName分组
        Hashtable<Pair<String, String>, List<DBValParam>> hashtable = CommonUtil.groupDBValParamByTableNameAndTagName(dbValParamList);
        for (Pair<String, String> pair : hashtable.keySet()) {
            String tableName = pair.getKey();
            String tagName = pair.getValue();
            List<DBValParam> dbValParams = hashtable.get(pair);

            int times = dbValParams.size() / splitSizeRealTime;
            if ((dbValParams.size() % splitSizeRealTime) != 0) times++;

            int fromIndex, toIndex;
            for (int i = 0; i < times; i++) {
                fromIndex = i * splitSizeRealTime;
                toIndex = (i + 1) * splitSizeRealTime;
                if (toIndex > dbValParams.size()) toIndex = dbValParams.size();

                List<DBVal> dbValListPartial = getDataListAtTimeRange(System.currentTimeMillis() - lookback,
                        System.currentTimeMillis(), tableName, tagName,
                        dbValParams.subList(fromIndex, toIndex), true);

                if (!dbValListPartial.isEmpty()) {
                    // 按照传参的tagValues进行排序(原结果集按照fieldName排序)。需用LinkedHashSet才能保证添加顺序，而非HashSet(乱序)
                    // 若获取全部对象，则不筛选
                    Set<String> tagValues = new LinkedHashSet<>();
                    for (DBValParam dbValParam : dbValParams.subList(fromIndex, toIndex)) {
                        if (dbValParam.getTagValue().isEmpty() || dbValParam.getTagValue().equals("*")) {
                            tagValues.clear();
                            break;
                        } else {
                            tagValues.add(dbValParam.getTagValue());
                        }
                    }

                    results.addAll(turnListOrdered(tagValues, dbValListPartial));
                }
            }
        }

        return results;
    }

    @Override
    public Map<String, List<DBVal>> getHistMultiTagValsFast(@NonNull List<DBValParam> dbValParamList,
                                                            long start, long end, int step) {
        Map<String, List<DBVal>> dbValMapList = new HashMap<>();

        // 将dbValParamList依据tableName 和 tagName分组
        Hashtable<Pair<String, String>, List<DBValParam>> hashtable = CommonUtil.groupDBValParamByTableNameAndTagName(dbValParamList);
        for (Pair<String, String> pair : hashtable.keySet()) {
            String tableName = pair.getKey();
            String tagName = pair.getValue();
            List<DBValParam> dbValParams = hashtable.get(pair);

            for (DBValParam dbVal : dbValParams) {
                //TODO 按照字段分类使用正则表达式构建查询语句？
                List<DBVal> dbVals = getHistSnap(dbVal, start, end, step);

                // 将结果按照tagValue排序
                if (dbValMapList.containsKey(dbVal.getTagValue())) {
                    dbValMapList.get(dbVal.getTagValue()).addAll(dbVals);
                } else {
                    dbValMapList.put(dbVal.getTagValue(), dbVals);
                }
            }
        }
        return dbValMapList;
    }

    @Override
    public List<DBVal> getHistSnap(@NonNull DBValParam dbValParam, long start, long end, long step) {
        List<DBVal> dbValList = getDataAtTimeRangeListWithStep(dbValParam, start, end, step);
        return dbValList;
    }

    @Override
    public List<DBVal> getHistSnap(DBValParam dbValParam, long startTime, long endTime, long period, long lookBack) {
        return null;
    }

    /**
     * @param dbValParam
     * @param start
     * @param end
     * @param step
     * @return java.util.List<db.temp.TempDBVal>
     * @description 被getHistSnap调用，查询指定fieldName下某段时间间隔结果集，过滤条件为：tagName=tagValue
     * @dateTime 2023/2/8 16:45
     */
    protected List<DBVal> getDataAtTimeRangeListWithStep(DBValParam dbValParam, long start, long end, long step) {
        if (StringUtils.isBlank(dbValParam.getTagValue()) || StringUtils.isBlank(dbValParam.getTagName())) {
            log.info("dbValParam: tagValue/tagName is empty!");
            return null;
        }

        // tagName和fieldName将采取枚举类型进行传参，无需trim操作
        String tagValue;
        if (this.isTrimTagValue()) {
            tagValue = trimStrong(dbValParam.getTagValue());
        } else {
            tagValue = dbValParam.getTagValue();
        }
        // List<TempDBVal> dbValList = new ArrayList<>();
        // String queryUrl = String.format("http://%s:%d//api/v1/query_range?query=%s{%s=\"%s\"," +
        //                 "status=\"%d\"}&start=%f&end=%f&step=%dms",
        //         host, port, dbValParam.getFieldName(), dbValParam.getTagName(), tagValue,
        //         ValueStatus.VALID.getValue(), start/1000.0, end/1000.0, step);
//
        // //TODO 数据查询时间间隔精度存在问题
        // System.out.println(queryUrl);
        // try {
        //     dbValList = TempSendRequest.getDataInTimeRange(queryUrl, dbValParam.getTagName());
        // } catch (Exception e) {
        //     e.printStackTrace();
        // }
        // return dbValList;
        return null;
    }

    @Override
    public List<DBVal> getHistRaw(@NonNull DBValParam dbValParam, long start, long end) {
        return getDataAtTimeRangeList(dbValParam, start, end);
    }

    /**
     * @param dbValParam
     * @param start
     * @param end
     * @return java.util.List<db.temp.TempDBVal>
     * @description 被getHistRaw调用，将start-end时间范围内数据，满足dbVal过滤条件的数据全部返回
     * @dateTime 2023/2/8 18:45
     */
    protected List<DBVal> getDataAtTimeRangeList(DBValParam dbValParam, long start, long end) {
        List<DBVal> results = new ArrayList<>();

        if (start > end) {
            log.info(String.format("The query time range is empty!"));
            return results;
        } else if (start == end) end += 1;        // 左闭右开

        if (StringUtils.isBlank(dbValParam.getTagName())) {
            log.info(String.format("tagName is empty!"));
            return results;
        }
        String tagValue = dbValParam.getTagValue();
        if (tagValue.isEmpty()) tagValue = "*";

        SpentTimeCalculator stc = SpentTimeCalculator.create().begin();
        String fluxQuery;
        if (tagValue.equals("*")) {
            fluxQuery = String.format("from(bucket:\"%s\")  |> range(start: %d, stop: %d) " +
                            "|> filter(fn: (r) => (r._measurement == \"%s\" and r.%s =~ /[\\s\\S*]/))",
                    this.bucket, start, end, dbValParam.getTableName(), dbValParam.getTagName());
        } else {
            fluxQuery = String.format("from(bucket:\"%s\")  |> range(start: %d, stop: %d) " +
                            "|> filter(fn: (r) => (r._measurement == \"%s\" and r.%s == \"%s\"))",
                    this.bucket, start, end, dbValParam.getTableName(), dbValParam.getTagName(), tagValue);
        }

        //QueryApi queryApi = influxDBClient.getQueryApi();
        //List<FluxTable> tables = queryApi.query(fluxQuery);
        List<FluxTable> tables = runFluxWithHint(fluxQuery, this.hint);

        Hashtable<String, SortedMap<Long, DBVal>> dbValHashtable = ParseData.parseFluxTableToDBVal(tables,
                dbValParam.getTagName());
        stc.end();
        log.info(String.format("queryLast tagName:%s tagValue:%s beginDate:%s endDate:%s spendTime:%d(ms)",
                dbValParam.getTagName(), dbValParam.getTagValue(),
                CommonUtil.uTCMilliSecondsToDateStringWithMs(start),
                CommonUtil.uTCMilliSecondsToDateString(end), stc.getSpendTime()));
        logSlowQuery("queryLast", fluxQuery, stc.getSpendTime());

        if (dbValHashtable.isEmpty()) return results;

        dbValHashtable.values().forEach(timeSery -> {
            timeSery.values().forEach(dbVal -> results.add((DBVal) dbVal));
        });

        return results;
    }

    @Override
    public List<DBVal> getHistRaw(@NonNull List<DBValParam> dbValParamList, long start, long end) {
        List<DBVal> results = new ArrayList<>();

        // 为dbValList分组
        Hashtable<Pair<String, String>, List<DBValParam>> hashtable = CommonUtil.groupDBValParamByTableNameAndTagName(dbValParamList);
        for (Pair<String, String> pair : hashtable.keySet()) {
            String tableName = pair.getKey();
            String tagName = pair.getValue();
            List<DBValParam> dbValParams = hashtable.get(pair);

            // 针对不同tableName 和 tagName分组查找数据
            List<DBVal> dbVals = getDataListAtTimeRange(start, end, tableName, tagName, dbValParams, false);

            if (dbVals.isEmpty()) continue;

            Set<String> tagValues = new LinkedHashSet<>();
            for (DBValParam dbValParam : dbValParams) {
                if (dbValParam.getTagValue().isEmpty() || dbValParam.getTagValue().equals("*")) {
                    tagValues.clear();
                    break;
                } else {
                    tagValues.add(dbValParam.getTagValue());
                }
            }

            results.addAll(turnListOrdered(tagValues, dbVals));
        }
        return results;
    }

    @Override
    public List<DBVal> getHistInstantRaw(List<DBValParam> dbVals, long time) {
        return null;
    }


    @Override
    public DBVal getRTMinValue(DBValParam dbValParam, long startTime, long endTime) {
        return this.getAggValueOverTime(AggFunction.MIN.getFunc(), dbValParam, startTime, endTime);
    }

    @Override
    public DBVal getRTMaxValue(DBValParam dbValParam, long startTime, long endTime) {
        return this.getAggValueOverTime(AggFunction.MAX.getFunc(), dbValParam, startTime, endTime);
    }

    @Override
    public DBVal getRTAvgValue(DBValParam dbValParam, long startTime, long endTime) {
        return this.getAggValueOverTime(AggFunction.MEAN.getFunc(), dbValParam, startTime, endTime);
    }

    @Override
    public AggCountResult getRTCountValue(DBValParam dbValParam, long startTime, long endTime) {
        DBVal dbVal = this.getAggValueOverTime(AggFunction.COUNT.getFunc(), dbValParam, startTime, endTime);
        if (dbVal.isValueValid()) {
            Double count = (Double) dbVal.getFieldValues().iterator().next();
            return AggCountResult.AggCountResultBuilder.anResult()
                    .withTableName(dbVal.getTableName())
                    .withTagName(dbVal.getTagName())
                    .withTagValue(dbVal.getTagValue())
                    .withCount(count.longValue())
                    .build();
        }

        return AggCountResult.AggCountResultBuilder.anResult().setResultInvalid().build();
    }

    /**
     * @param aggMethod
     * @param dbValParam
     * @param beginDate
     * @param endDate
     * @return db.temp.TempDBVal
     * @description 查询指定tagName下，指定时间段的聚合结果
     * @dateTime 2023/2/9 20:16
     */
    protected DBVal getAggValueOverTime(String aggMethod, DBValParam dbValParam, long beginDate, long endDate) {
        DBVal result;
        if (beginDate > endDate) {
            result = DBVal.DBValBuilder.anDBVal()
                    .withValueStatus(ValueStatus.INVALID)
                    .build();
            return result;
        }
        if (StringUtils.isBlank(dbValParam.getTagValue()) || StringUtils.isBlank(dbValParam.getTableName()) ||
                StringUtils.isBlank(dbValParam.getTagName())) {
            log.info(String.format("tagValue/tagName/tableName is empty!"));
            result = DBVal.DBValBuilder.anDBVal()
                    .withValueStatus(ValueStatus.INVALID)
                    .build();
            return result;
        }

        try {
            String tagValue = dbValParam.getTagValue();
            if (this.isTrimTagValue()) {
                tagValue = trimStrong(tagValue);
            }

            // 聚合是指将该段时间范围内数据按照聚合函数进行计算，influxdb聚合粒度为s
            SpentTimeCalculator stc = SpentTimeCalculator.create().begin();
            String fluxQuery = String.format("from(bucket:\"%s\") |> range(start: %d, stop: %d)" +
                            "  |> filter(fn: (r) => (r._measurement == \"%s\" and r.%s == \"%s\")) " +
                            "  |> aggregateWindow(every: %d%s, offset: %s, fn: %s)",
                    this.bucket, beginDate / 1000, endDate / 1000, dbValParam.getTableName(), dbValParam.getTagName(),
                    tagValue, (endDate - beginDate) / 1000, "s", offset, aggMethod);


            //QueryApi queryApi = influxDBClient.getQueryApi();
            log.info(fluxQuery);
            //List<FluxTable> tables = queryApi.query(fluxQuery);
            List<FluxTable> tables = runFluxWithHint(fluxQuery, this.hint);

            log.info(fluxQuery);
            Hashtable<String, SortedMap<Long, DBVal>> dbValHashtable = ParseData.parseFluxTableToDBVal(tables,
                    dbValParam.getTagName());

            stc.end();
            log.info(String.format("queryLast tableName:%s tagName:%s beginDate:%s endDate:%s " +
                            "spendTime:%d(ms)",
                    dbValParam.getTableName(), dbValParam.getTagName(),
                    CommonUtil.uTCMilliSecondsToDateStringWithMs(beginDate),
                    CommonUtil.uTCMilliSecondsToDateString(endDate), stc.getSpendTime()));
            logSlowQuery("queryLast", fluxQuery, stc.getSpendTime());

            // 将dbValHashtable转为dbValList，且按照传参顺序将数据存入列表中
            if (!dbValHashtable.containsKey(dbValParam.getTagValue())) {
                result = DBVal.DBValBuilder.anDBVal()
                        .withValueStatus(ValueStatus.INVALID)
                        .build();
                return result;
            }

            result = dbValHashtable.get(dbValParam.getTagValue()).values().iterator().next();
        } catch (Exception e) {
            log.warn(String.format("retrieval error: tableName=%s, tagName=%s, aggMethod=%s",
                    dbValParam.getTableName(), dbValParam.getTagName(), aggMethod));
            result = DBVal.DBValBuilder.anDBVal()
                    .withValueStatus(ValueStatus.INVALID)
                    .build();
        }
        return result;
    }

    // 降采样查询
    @Override
    public List<DBVal> downSamplingQuery(AggFunctionType aggFunctionType, long timeGranularity,
                                         DBValParam dbValParam, long beginDate, long endDate) {
        List<DBVal> results = new ArrayList<>();

        if (beginDate > endDate) {
            return results;
        }
        if (StringUtils.isBlank(dbValParam.getTagValue()) || StringUtils.isBlank(dbValParam.getTableName()) ||
                StringUtils.isBlank(dbValParam.getTagName())) {
            log.info(String.format("tagValue/tagName/tableName is empty!"));
            return results;
        }

        try {
            String tagValue = dbValParam.getTagValue();
            if (this.isTrimTagValue()) {
                tagValue = trimStrong(tagValue);
            }

            // 聚合是指将该段时间范围内数据按照聚合函数进行计算，influxdb聚合粒度为s
            SpentTimeCalculator stc = SpentTimeCalculator.create().begin();
            String fluxQuery = String.format("from(bucket:\"%s\") |> range(start: %d, stop: %d)" +
                            "  |> filter(fn: (r) => (r._measurement == \"%s\" and r.%s == \"%s\")) " +
                            "  |> aggregateWindow(every: %d%s, offset: %s, fn: %s)",
                    this.bucket, beginDate / 1000, endDate / 1000, dbValParam.getTableName(), dbValParam.getTagName(),
                    tagValue, timeGranularity / 1000, "s", offset, AggFunction.valueOf(aggFunctionType.getFunc()).getFunc());


            //QueryApi queryApi = influxDBClient.getQueryApi();
            //List<FluxTable> tables = queryApi.query(fluxQuery);
            List<FluxTable> tables = runFluxWithHint(fluxQuery, this.hint);

            Hashtable<String, SortedMap<Long, DBVal>> dbValHashtable = ParseData.parseFluxTableToDBVal(tables,
                    dbValParam.getTagName());

            stc.end();
            log.info(String.format("queryLast tableName:%s tagName:%s beginDate:%s endDate:%s " +
                            "spendTime:%d(ms)",
                    dbValParam.getTableName(), dbValParam.getTagName(),
                    CommonUtil.uTCMilliSecondsToDateStringWithMs(beginDate),
                    CommonUtil.uTCMilliSecondsToDateString(endDate), stc.getSpendTime()));
            logSlowQuery("queryLast", fluxQuery, stc.getSpendTime());

            if (dbValHashtable.isEmpty()) return results;

            // 将dbValHashtable转为dbValList
            dbValHashtable.values().forEach(timeSery -> {
                timeSery.values().forEach(dbVal -> results.add((DBVal) dbVal));
            });
        } catch (Exception e) {
            log.warn(String.format("retrieval error: tableName=%s, tagName=%s, aggMethod=%s",
                    dbValParam.getTableName(), dbValParam.getTagName(), aggFunctionType.getFunc()));
        }

        return results;
    }


    // *****************************************************************************************************
    /**
     * 针对单个id，给定时间范围，给定分组窗口粒度，对每个分组窗口中的给定字段fieldName值进行aggFunction聚合运算。
     * @param id 待查询的id。
     * @param beginDate 开始时间，UNIX毫秒。
     * @param endDate 结束时间，UNIX毫秒。
     * @param granularityValue 分组粒度的数值部分。
     * @param granularity 枚举型，分组粒度的单位部分。
     * @param aggFunction 枚举型，聚合运算使用的函数。
     * @param driverLocationFieldName 枚举型，查询的字段名。
     * @return id在给定时间范围内的PointData经过分组聚合得到的列表。
     */
    /*public List<PointDataWithSingleValue> aggTimeRangeByWindow(String id, long beginDate, long endDate,
                                                               long granularityValue, Granularity granularity,
                                                               AggFunction aggFunction, DriverLocationFieldName driverLocationFieldName) {
        SpentTimeCalculator stc = SpentTimeCalculator.create().begin();
        String fluxQuery = String.format("from(bucket:\"%s\") |> range(start: %d, stop: %d)" +
                        "  |> filter(fn: (r) => (r._measurement == \"%s\" and r.id == \"%s\" and r._field == \"%s\")) " +
                        "  |> aggregateWindow(every: %d%s, offset: %s, fn: %s)",
                this.bucket, beginDate/1000, endDate/1000, measurementName, id, driverLocationFieldName.getValue(),
                granularityValue, granularity.getValue(), offset, aggFunction.getFunc());

        QueryApi queryApi = influxDBClient.getQueryApi();
        List<FluxTable> tables = queryApi.query(fluxQuery);
        Hashtable<String, SortedMap<Long,PointDataWithSingleValue>> pointDataHashtable = ParseData.parseWithSingleValue(tables);

        stc.end();
        log.info(String.format("aggTimeRangeByWindow id:%s beginDate:%s endDate:%s granularityValue:%d granularity:%s" +
                        " aggFunction:%s driverLocationFieldName:%s  spendTime:%d(ms)",
                id, CommonUtil.uTCMilliSecondsToDateStringWithMs(beginDate),
                CommonUtil.uTCMilliSecondsToDateString(endDate),
                granularityValue, granularity.getValue(),
                aggFunction.getFunc(), driverLocationFieldName.getValue(),
                stc.getSpendTime()));
        logSlowQuery("aggTimeRangeByWindow", fluxQuery, stc.getSpendTime());

        SortedMap timeSery = pointDataHashtable.get(id);
        if (timeSery == null) {
            return new ArrayList<>();
        }
        else {
            return new ArrayList<>(timeSery.values());
        }
    }

    *//**
     * 针对一系列id，给定时间范围，给定分组窗口粒度，对每个分组窗口中的给定字段fieldName值进行aggFunction聚合运算。
     * @param ids 待查询的id的列表。
     * @param beginDate 开始时间，UNIX毫秒。
     * @param endDate 结束时间，UNIX毫秒。
     * @param granularityValue 枚举型，分组粒度数值部分。
     * @param granularity 枚举型，分组粒度单位部分。
     * @param aggFunction 枚举型，聚合运算的函数。
     * @param driverLocationFieldName 枚举型，查询的字段名。
     * @return {@literal HashTable<id, SortedMap<UNIX毫秒时间戳, 时间戳对应的PointDataWithSingleValue>>}。
     *//*
    public Hashtable<String, SortedMap<Long,PointDataWithSingleValue>> aggTimeRangeByWindow(List<String> ids,
                                                                                            long beginDate, long endDate,
                                                                                            long granularityValue, Granularity granularity,
                                                                                            AggFunction aggFunction, DriverLocationFieldName driverLocationFieldName) {
        SpentTimeCalculator stc = SpentTimeCalculator.create().begin();
        String fluxQuery = String.format("from(bucket:\"%s\") |> range(start: %d, stop: %d)" +
                        "  |> filter(fn: (r) => (r._measurement == \"%s\" and %s and r._field == \"%s\")) " +
                        "  |> aggregateWindow(every: %d%s, offset: %s, fn: %s)",
                this.bucket, beginDate/1000, endDate/1000, measurementName, ParseData.idsToFilterString(ids),
                driverLocationFieldName.getValue(), granularityValue, granularity.getValue(), offset, aggFunction.getFunc());

        QueryApi queryApi = influxDBClient.getQueryApi();
        List<FluxTable> tables = queryApi.query(fluxQuery);
        Hashtable<String, SortedMap<Long,PointDataWithSingleValue>> pointDataHashtable = ParseData.parseWithSingleValue(tables);

        stc.end();
        log.info(String.format("aggTimeRangeByWindow ids:%s beginDate:%s endDate:%s granularityValue:%d granularity:%s" +
                        "  aggFunction:%s driverLocationFieldName:%s spendTime:%d(ms)",
                ids, CommonUtil.uTCMilliSecondsToDateStringWithMs(beginDate),
                CommonUtil.uTCMilliSecondsToDateString(endDate),
                granularityValue, granularity.getValue(),
                aggFunction.getFunc(), driverLocationFieldName.getValue(),
                stc.getSpendTime()));
        logSlowQuery("aggTimeRangeByWindow", fluxQuery, stc.getSpendTime());

        return pointDataHashtable;
    }



    *//**
     * 针对单个id，给定时间范围，对给定字段fieldName值进行aggFunction聚合运算。
     * @param id 待查询的id。
     * @param beginDate 开始时间，UNIX毫秒。
     * @param endDate 结束时间，UNIX毫秒。
     * @param aggFunction 枚举型，聚合运算使用的函数。
     * @param driverLocationFieldName 枚举型，查询的字段名。
     * @return id在给定时间范围内的PointData经过分组聚合得到的列表。
     *//*
    public PointDataWithSingleValue aggTimeRange(String id, long beginDate, long endDate,
                                                 AggFunction aggFunction, DriverLocationFieldName driverLocationFieldName) {
        SpentTimeCalculator stc = SpentTimeCalculator.create().begin();
        String fluxQuery = String.format("from(bucket:\"%s\") |> range(start: %d, stop: %d)" +
                        "  |> filter(fn: (r) => (r._measurement == \"%s\" and r.id == \"%s\" and r._field == \"%s\")) " +
                        "  |> aggregateWindow(every: inf, fn: %s)",
                this.bucket, beginDate/1000, endDate/1000, measurementName, id, driverLocationFieldName.getValue(),
                aggFunction.getFunc());

        QueryApi queryApi = influxDBClient.getQueryApi();
        List<FluxTable> tables = queryApi.query(fluxQuery);
        Hashtable<String, PointDataWithSingleValue> pointDataHashtable = ParseData.parseIDAndSingleValue(tables);

        stc.end();
        log.info(String.format("aggTimeRange id:%s beginDate:%s endDate:%s aggFunction:%s driverLocationFieldName:%s spendTime:%d(ms)",
                id, CommonUtil.uTCMilliSecondsToDateStringWithMs(beginDate),
                CommonUtil.uTCMilliSecondsToDateString(endDate),
                aggFunction.getFunc(), driverLocationFieldName.getValue(),
                stc.getSpendTime()));
        logSlowQuery("aggTimeRange", fluxQuery, stc.getSpendTime());

        PointDataWithSingleValue pintDataWithSingleValue = pointDataHashtable.get(id);
        if (pintDataWithSingleValue == null) {
            pintDataWithSingleValue = new PointDataWithSingleValue();
            pintDataWithSingleValue.setValueStatus(ValueStatus.INVALID);
            return pintDataWithSingleValue;
        }
        else {
            return pintDataWithSingleValue;
        }
    }


    *//**
     * 针对一系列id，给定时间范围，对给定字段fieldName值进行aggFunction聚合运算。
     * @param ids 待查询的id的列表。
     * @param beginDate 开始时间，UNIX毫秒。
     * @param endDate 结束时间，UNIX毫秒。
     * @param aggFunction 枚举型，聚合运算的函数。
     * @param driverLocationFieldName 枚举型，查询的字段名。
     * @return {@literal HashTable<id, SortedMap<UNIX毫秒时间戳, 时间戳对应的PointDataWithSingleValue>>}。
     *//*
    public Hashtable<String, PointDataWithSingleValue> aggTimeRange(List<String> ids, long beginDate, long endDate,
                                                                    AggFunction aggFunction, DriverLocationFieldName driverLocationFieldName) {
        SpentTimeCalculator stc = SpentTimeCalculator.create().begin();
        String fluxQuery = String.format("from(bucket:\"%s\") |> range(start: %d, stop: %d)" +
                        "  |> filter(fn: (r) => (r._measurement == \"%s\" and %s and r._field == \"%s\")) " +
                        "  |> aggregateWindow(every: inf, fn: %s)",
                this.bucket, beginDate/1000, endDate/1000, measurementName, ParseData.idsToFilterString(ids),
                driverLocationFieldName.getValue(),  aggFunction.getFunc());

        QueryApi queryApi = influxDBClient.getQueryApi();
        List<FluxTable> tables = queryApi.query(fluxQuery);
        Hashtable<String, PointDataWithSingleValue> pointDataHashtable = ParseData.parseIDAndSingleValue(tables);

        stc.end();
        log.info(String.format("aggTimeRange ids:%s beginDate:%s endDate:%s aggFunction:%s driverLocationFieldName:%s spendTime:%d(ms)",
                ids, CommonUtil.uTCMilliSecondsToDateStringWithMs(beginDate),
                CommonUtil.uTCMilliSecondsToDateString(endDate),
                aggFunction.getFunc(), driverLocationFieldName.getValue(),
                stc.getSpendTime()));
        logSlowQuery("aggTimeRange", fluxQuery, stc.getSpendTime());

        return pointDataHashtable;
    }


    *//**
     * 针对一系列id，给定时间范围，分组窗口粒度为天，给定分组窗口[beginSecondInADay,endSecondInADay]中的数据。
     * 对每个分组窗口中的给定字段fieldName值进行aggFunction聚合运算。
     * @param ids 待查询的id的列表。
     * @param beginDate 开始时间，UNIX毫秒。
     * @param endDate 结束时间，UNIX毫秒。
     * @param aggFunction 枚举型，聚合函数。
     * @param driverLocationFieldName 枚举型，查询的字段名。
     * @param beginSecondInADay 一天内的某个时间窗口的开始时间，单位为秒，如 300 (= 5 * 60) 表示 00:05:00。
     * @param endSecondInADay 一天内的某个时间窗口的结束时间，单位为秒，如 900 (= 15 * 60) 表示 00:15:00。
     * @return {@literal HashTable<id, SortedMap<UNIX毫秒时间戳, 时间戳对应的PointDataWithSingleValue>>}。
     *//*
    public Hashtable<String, SortedMap<Long,PointDataWithSingleValue>> aggTimeRangeByDayInSecondRange(List<String> ids,
                                                                                                      long beginDate, long endDate,
                                                                                                      AggFunction aggFunction, DriverLocationFieldName driverLocationFieldName,
                                                                                                      int beginSecondInADay,
                                                                                                      int endSecondInADay) {
        SpentTimeCalculator stc = SpentTimeCalculator.create().begin();
        String fluxQuery = String.format("import \"date\"\nfrom(bucket:\"%s\") |> range(start: %d, stop: %d)" +
                        "  |> filter(fn: (r) => (r._measurement == \"%s\" and r._field == \"%s\" and %s and %s)) " +
                        "  |> aggregateWindow(every: 1d, offset: %s, fn: %s)",
                this.bucket, beginDate/1000, endDate/1000, measurementName, driverLocationFieldName.getValue(),
                ParseData.idsToFilterString(ids),
                ParseData.timeInSecondsRangeFilterString(beginSecondInADay, endSecondInADay),
                offset,
                aggFunction.getFunc());

        QueryApi queryApi = influxDBClient.getQueryApi();
        List<FluxTable> tables = queryApi.query(fluxQuery);
        Hashtable<String, SortedMap<Long,PointDataWithSingleValue>> pointDataHashtable =
                ParseData.parseWithSingleValue(tables);

        stc.end();
        log.info(String.format("aggTimeRangeByDayInSecondRange ids:%s beginDate:%s endDate:%s " +
                        "aggFunction:%s driverLocationFieldName:%s beginSecondInADay:%d endSecondInADay:%d spendTime:%d(ms)",
                ids, CommonUtil.uTCMilliSecondsToDateStringWithMs(beginDate),
                CommonUtil.uTCMilliSecondsToDateString(endDate),
                aggFunction.getFunc(), driverLocationFieldName.getValue(), beginSecondInADay, endSecondInADay,
                stc.getSpendTime()));
        logSlowQuery("aggTimeRangeByDayInSecondRange", fluxQuery, stc.getSpendTime());

        return pointDataHashtable;
    }

    public void sendPointDataToDB(String sentences) {
        try {
            String sendDataUrl = "http://" + Globals.HOST + ":" + Globals.port + "/api/v2/write?"
                    + "org=orgTest&bucket=driverLocation&precision=ms";
            URL realUrl = new URL(sendDataUrl);
            URLConnection conn = realUrl.openConnection();
            conn.setRequestProperty("accept", "application/json");
            conn.setRequestProperty("Content-Type", "text/plain; charset=utf-8");
            conn.setRequestProperty("Authorization", "Token " + Globals.TOKEN);
            conn.setDoOutput(true);
            conn.setDoInput(true);
            // fill and send content
            DataOutputStream dos = new DataOutputStream(conn.getOutputStream());
            dos.write(sentences.getBytes());
            dos.flush();
            // get response (Do not comment this line, or the data insertion will be failed)
            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
//            String line;
//            while ((line = in.readLine()) != null) {
//                result += line;
//            }
//            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    *//**
     *
     *
     *
     *
     *
     *
     *
     *//*
     *//*public Hashtable<String, SortedMap<Long,PointDataWithSingleValue>> queryFlowByWindow(List<String> ids,
                                                                                         long beginDate, long endDate,
                                                                                         long granularityValue,
                                                                                         Granularity granularity) {
        针对一系列id，给定时间范围和分组粒度，查询对应的流量值。
        @param ids 待查询的id的列表。
        @param beginDate 开始时间，UNIX毫秒。
        @param endDate 结束时间，UNIX毫秒。
        @param granularityValue 分组粒度数值部分。
        @param granularity 枚举型，分组粒度单位部分。
        @return {@literal HashTable<id, SortedMap<UNIX毫秒时间戳, 时间戳对应的PointDataWithSingleValue>>}。

        SpentTimeCalculator stc = SpentTimeCalculator.create().begin();
        long querBeginDate = beginDate - this.increaseLookBack;
        String fluxQuery = String.format("from(bucket:\"%s\") |> range(start: %d, stop: %d)" +
                        "  |> filter(fn: (r) => (r._measurement == \"%s\" and %s and r._field == \"%s\")) " +
                        "  |> difference()" +
                        "  |> aggregateWindow(every: %d%s, offset: %s, fn: sum)",
                this.bucket, querBeginDate/1000, endDate/1000, measurementName, TempParseData.idsToFilterString(ids),
                DriverLocationFieldName.TOTAL_FLOW.getValue(), granularityValue,
                granularity.getValue(),
                offset,
                granularity.getValue());

        QueryApi queryApi = influxDBClient.getQueryApi();
        List<FluxTable> tables = queryApi.query(fluxQuery);
        Hashtable<String, SortedMap<Long,PointDataWithSingleValue>> pointDataHashtable =
                TempParseData.parseWithSingleValue(tables);
        Hashtable<String, SortedMap<Long, PointDataWithSingleValue>> result =
                new Hashtable<>();

        for (String id : pointDataHashtable.keySet()) {
            SortedMap<Long, PointDataWithSingleValue> timeSery = pointDataHashtable.get(id);
            result.put(id, greaterThanTime(timeSery, beginDate));
        }

        stc.end();
        log.info(String.format("queryFlowByWindow ids:%s beginDate:%s endDate:%s granularityValue:%d granularity:%s" +
                        " spendTime:%d(ms)",
                ids, CommonCommonUtil.uTCMilliSecondsToDateStringWithMs(beginDate),
                CommonUtil.uTCMilliSecondsToDateString(endDate),
                granularityValue, granularity.getValue(),
                stc.getSpendTime()));
        logSlowQuery("queryFlowByWindow", fluxQuery, stc.getSpendTime());
        return result;
    }*//*

    protected SortedMap<Long, PointDataWithSingleValue> greaterThanTime(SortedMap<Long, PointDataWithSingleValue> timeSery,
                                                                        long compareTime) {
        SortedMap<Long, PointDataWithSingleValue> result = new TreeMap<>();
        for (Long time : timeSery.keySet()) {
            if(time<=compareTime) {
                continue;
            }
            result.put(time, timeSery.get(time));
        }
        return result;
    }

    *//**
     * 针对单个id，给定时间范围和分组粒度，查询对应的流量值。
     * @param id 待查询的id。
     * @param beginDate 开始时间，UNIX毫秒。
     * @param endDate 结束时间，UNIX毫秒。
     * @param granularityValue 分组粒度数值部分。
     * @param granularity 枚举型，分组粒度单位部分。
     * @return {@literal List<UNIX毫秒时间戳，对应的PointDataWithSingleValue>。}<br>
     * PointDataWithSingleValue类有四个public属性：id、pointName、time、value。
     *//*
     *//*public List<PointDataWithSingleValue> queryFlowByWindow(String id, long beginDate, long endDate,
                                                            long granularityValue, Granularity granularity) {
        SpentTimeCalculator stc = SpentTimeCalculator.create().begin();
        long querBeginDate = beginDate - this.increaseLookBack;
        String fluxQuery = String.format("from(bucket:\"%s\") |> range(start: %d, stop: %d)" +
                        "  |> filter(fn: (r) => (r._measurement == \"%s\" and r.id == \"%s\" and r._field == \"%s\")) " +
                        "  |> difference()" +
                        "  |> aggregateWindow(every: %d%s, offset: %s, fn: sum)",
                this.bucket, querBeginDate/1000, endDate/1000, measurementName, id, DriverLocationFieldName.TOTAL_FLOW.getValue(),
                granularityValue,
                granularity.getValue(),
                offset,
                granularity.getValue());

        QueryApi queryApi = influxDBClient.getQueryApi();
        List<FluxTable> tables = queryApi.query(fluxQuery);
        Hashtable<String, SortedMap<Long,PointDataWithSingleValue>> pointDataHashtable =
                TempParseData.parseWithSingleValue(tables);

        stc.end();
        log.info(String.format("queryFlowByWindow id:%s beginDate:%s endDate:%s granularityValue:%d granularity:%s" +
                        "spendTime:%d(ms)",
                id, CommonCommonUtil.uTCMilliSecondsToDateStringWithMs(beginDate),
                CommonUtil.uTCMilliSecondsToDateString(endDate),
                granularityValue, granularity.getValue(),
                stc.getSpendTime()));
        logSlowQuery("queryFlowByWindow", fluxQuery, stc.getSpendTime());

        SortedMap<Long, PointDataWithSingleValue> timeSery = pointDataHashtable.get(id);
        if (timeSery == null) {
            return new ArrayList<>();
        }
        else {
            SortedMap<Long, PointDataWithSingleValue> result = greaterThanTime(timeSery, beginDate);
            return new ArrayList<>(result.values());
        }
    }*//*

     *//**
     * 针对单个id，给定时间范围，查询对应时间范围内使用的流量值。
     * @param id 待查询的id。
     * @param beginDate 开始时间，UNIX毫秒。
     * @param endDate 结束时间，UNIX毫秒。
     * @return {@literal FlowInfo。}<br>
     * PointDataWithSingleValue类有四个public属性：id、pointName、time、value。
     *//*
     *//*public FlowInfo queryFlowInTimeRange(String id, long beginDate, long endDate) {
        SpentTimeCalculator stc = SpentTimeCalculator.create().begin();
        PointDataWithSingleValue flowPointData = new PointDataWithSingleValue();
        flowPointData.setId(id);
        flowPointData.setValueStatusInvalid();
        FlowInfo flowInfo = new FlowInfo(id, beginDate, endDate);
        flowInfo.setFlow(flowPointData);

        long querBeginDate = beginDate - this.increaseLookBack;
        PointData lastPointDataInPrevWindow = this.queryLastInTimeRange(id, querBeginDate, beginDate);
        PointData lastPointDataInCurWindow = this.queryLastInTimeRange(id, beginDate, endDate);
        flowInfo.setLastPointDataInPrevWindow(lastPointDataInPrevWindow);
        flowInfo.setLastPointDataInCurWindow(lastPointDataInCurWindow);

        if (!lastPointDataInPrevWindow.isValueValid() || lastPointDataInPrevWindow.getTotalFlow() == null) {
            return flowInfo;
        }

        if (!lastPointDataInCurWindow.isValueValid() || lastPointDataInCurWindow.getTotalFlow() == null) {
            return flowInfo;
        }

        flowPointData.setValue(lastPointDataInCurWindow.getTotalFlow()-lastPointDataInPrevWindow.getTotalFlow());
        flowPointData.setTime(lastPointDataInCurWindow.getTime());
        flowPointData.setValueStatusValid();

        stc.end();
        log.info(String.format("queryFlowInTimeRange id:%s beginDate:%s endDate:%s spendTime:%d(ms)",
                id, CommonCommonUtil.uTCMilliSecondsToDateStringWithMs(beginDate),
                CommonUtil.uTCMilliSecondsToDateString(endDate),
                stc.getSpendTime()));
        logSlowQuery("queryFlowInTimeRange",
                String.format("id:%s beginDate:%s endDate:%s", id,
                        CommonCommonUtil.uTCMilliSecondsToDateStringWithMs(beginDate),
                        CommonUtil.uTCMilliSecondsToDateString(endDate)),
                stc.getSpendTime());
        return flowInfo;
    }*//*

     *//**
     *
     *
     *
     *
     *
     *
     *//*
     *//*public Hashtable<String, FlowInfo> queryFlowInTimeRange(List<String> ids, long beginDate, long endDate) {
        SpentTimeCalculator stc = SpentTimeCalculator.create().begin();
        Hashtable<String, FlowInfo> result = new Hashtable<>();

        针对一系列id，给定时间范围，查询对应时间范围内使用的流量值。
        @param ids 待查询的id列表。
        @param beginDate 开始时间，UNIX毫秒。
        @param endDate 结束时间，UNIX毫秒。
        @return {@literal FlowInfo。}<br>
        PointDataWithSingleValue类有四个public属性：id、pointName、time、value。

        long querBeginDate = beginDate - this.increaseLookBack;
        Hashtable<String, PointData> lastPointDatasInPrevWindow = this.queryLastInTimeRange(ids, querBeginDate, beginDate);
        Hashtable<String, PointData> lastPointDatasInCurWindow = this.queryLastInTimeRange(ids, beginDate, endDate);

        PointData lastPointDataInPrevWindow;
        PointData lastPointDataInCurWindow;
        for (String id : ids) {
            PointDataWithSingleValue flowPointData = new PointDataWithSingleValue();
            flowPointData.setId(id);
            flowPointData.setValueStatusInvalid();
            FlowInfo flowInfo = new FlowInfo(id, beginDate, endDate);
            flowInfo.setFlow(flowPointData);

            lastPointDataInPrevWindow = lastPointDatasInPrevWindow.get(id);
            lastPointDataInCurWindow = lastPointDatasInCurWindow.get(id);
            flowInfo.setLastPointDataInPrevWindow(lastPointDataInPrevWindow);
            flowInfo.setLastPointDataInCurWindow(lastPointDataInCurWindow);

            if (lastPointDataInPrevWindow.isValueValid() && lastPointDataInCurWindow.isValueValid()) {
                flowPointData.setValue(lastPointDataInCurWindow.getTotalFlow()-lastPointDataInPrevWindow.getTotalFlow());
                flowPointData.setTime(lastPointDataInCurWindow.getTime());
                flowPointData.setValueStatusValid();
            }
            result.put(id, flowInfo);
        }

        stc.end();
        log.info(String.format("queryFlowInTimeRange ids:%s beginDate:%s endDate:%s spendTime:%d(ms)",
                ids, CommonCommonUtil.uTCMilliSecondsToDateStringWithMs(beginDate),
                CommonUtil.uTCMilliSecondsToDateString(endDate),
                stc.getSpendTime()));
        logSlowQuery("queryFlowInTimeRange",
                String.format("ids:%s beginDate:%s endDate:%s",
                        ids, CommonCommonUtil.uTCMilliSecondsToDateStringWithMs(beginDate),
                        CommonUtil.uTCMilliSecondsToDateString(endDate)),
                stc.getSpendTime());

        return result;
    }*//*

    public boolean deleteAllDataInTimeRange(long beginDate, long endDate) {
        DeleteApi deleteApi = influxDBClient.getDeleteApi();
        try {
            OffsetDateTime start = CommonUtil.toOffsetDateTime(beginDate);
            OffsetDateTime stop = CommonUtil.toOffsetDateTime(endDate);
            deleteApi.delete(start, stop, "", this.bucket, this.org);
            return true;
        } catch (InfluxException ie) {
            ie.printStackTrace();
            return false;
        }
    }


    *//**
     * 删除指定名称的bucket。
     * @param name bucket的名称。
     * @return 删除成功返回true，出现异常返回false。
     *//*
    public boolean deleteBucket(String name) {
        try {
            BucketsApi bucketsApi = influxDBClient.getBucketsApi();
            Bucket bucket = bucketsApi.findBucketByName(name);
            if (bucket == null) {
                return true;
            }
            bucketsApi.deleteBucket(bucket);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }


    *//**
     * 指定名称，创建新的bucket。
     * @param name 待创建的新的bucket的名称。
     * @param retentionPeriod 保留期，单位为毫秒。
     * @return 成功返回true，出现异常则为false。
     *//*
    public boolean createBucket(String name, long retentionPeriod) {
        try {
            OrganizationsApi organizationsApi = influxDBClient.getOrganizationsApi();
            BucketRetentionRules bucketRetentionRules = new BucketRetentionRules();
            bucketRetentionRules.setEverySeconds((int)retentionPeriod/1000);

            BucketsApi bucketsApi = influxDBClient.getBucketsApi();
            Bucket bucket = bucketsApi.createBucket(name, bucketRetentionRules, this.org);
            if (bucket != null) {
                return true;
            }
            return false;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }*/
}
