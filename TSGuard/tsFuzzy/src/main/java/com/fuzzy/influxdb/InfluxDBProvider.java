package com.fuzzy.influxdb;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.benchmark.util.HttpRequestEnum;
import com.fuzzy.*;
import com.fuzzy.common.DBMSCommon;
import com.fuzzy.common.constant.GlobalConstant;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.common.query.SQLQueryProvider;
import com.fuzzy.influxdb.feedback.InfluxDBQuerySynthesisFeedbackManager;
import com.fuzzy.influxdb.gen.*;
import com.fuzzy.influxdb.gen.InfluxDBShowStatementGenerator.ShowStatementType;
import com.fuzzy.influxdb.util.InfluxDBAuthorizationParams;
import com.google.auto.service.AutoService;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

@AutoService(DatabaseProvider.class)
public class InfluxDBProvider extends SQLProviderAdapter<InfluxDBGlobalState, InfluxDBOptions> {


    public InfluxDBProvider() {
        super(InfluxDBGlobalState.class, InfluxDBOptions.class);
    }

    enum Action implements AbstractAction<InfluxDBGlobalState> {
        CREATE_TABLE((g) -> {
            String tableName = DBMSCommon.createTableName(g.getSchema().getMaxTableIndex() + 1);
            return InfluxDBTableGenerator.generate(g, tableName);
        }),
        SHOW_DATABASES((g) -> new SQLQueryAdapter("q=SHOW DATABASES")),
        DELETE(InfluxDBDeleteGenerator::delete),
        INSERT(InfluxDBInsertGenerator::insertRow),
        UPDATE(InfluxDBUpdateGenerator::replaceRow),
        // SHOW 语句 -> 仅测试崩溃错误. 不测试逻辑错误
        SHOW_FIELD_KEYS((g) ->
                InfluxDBShowStatementGenerator.generateShowStatement(g, ShowStatementType.SHOW_FIELD_KEYS)),
        SHOW_FIELD_KEY_CARDINALITY((g) ->
                InfluxDBShowStatementGenerator.generateShowStatement(g, ShowStatementType.SHOW_FIELD_KEY_CARDINALITY)),
        SHOW_MEASUREMENTS((g) ->
                InfluxDBShowStatementGenerator.generateShowStatement(g, ShowStatementType.SHOW_MEASUREMENTS)),
        SHOW_SERIES((g) ->
                InfluxDBShowStatementGenerator.generateShowStatement(g, ShowStatementType.SHOW_SERIES)),
        SHOW_SERIES_CARDINALITY((g) ->
                InfluxDBShowStatementGenerator.generateShowStatement(g, ShowStatementType.SHOW_SERIES_CARDINALITY)),
        SHOW_TAG_KEYS((g) ->
                InfluxDBShowStatementGenerator.generateShowStatement(g, ShowStatementType.SHOW_TAG_KEYS)),
        SHOW_TAG_KEY_CARDINALITY((g) ->
                InfluxDBShowStatementGenerator.generateShowStatement(g, ShowStatementType.SHOW_TAG_KEY_CARDINALITY)),
        SHOW_TAG_VALUES((g) ->
                InfluxDBShowStatementGenerator.generateShowStatement(g, ShowStatementType.SHOW_TAG_VALUES)),
        SHOW_TAG_VALUES_CARDINALITY((g) ->
                InfluxDBShowStatementGenerator.generateShowStatement(g, ShowStatementType.SHOW_TAG_VALUES_CARDINALITY)),
        DROP_MEASUREMENT(InfluxDBDropMeasurementGenerator::generate);

        private final SQLQueryProvider<InfluxDBGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<InfluxDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(InfluxDBGlobalState globalState) throws Exception {
            return sqlQueryProvider.getQuery(globalState);
        }
    }

    private static int mapActions(InfluxDBGlobalState globalState, Action a) {
        // 执行权重划分
        Randomly r = globalState.getRandomly();
        int nrPerformed = 0;
        switch (a) {
            case CREATE_TABLE:
                nrPerformed = r.getInteger(0, 0);
                break;
            case SHOW_DATABASES:
                nrPerformed = r.getInteger(0, 1);
                break;
            case INSERT:
                nrPerformed = r.getInteger(9, globalState.getOptions().getMaxNumberInserts());
                break;
            case UPDATE:
                nrPerformed = 0;
//                if (globalState.usesTSAF())
//                    nrPerformed = r.getInteger(0, globalState.getOptions().getMaxNumberInserts() / 5);
                break;
            case DELETE:
                // TODO delete无法使用？
                nrPerformed = r.getInteger(0, 0);
                break;
            case SHOW_FIELD_KEYS:
            case SHOW_FIELD_KEY_CARDINALITY:
            case SHOW_MEASUREMENTS:
            case SHOW_SERIES:
            case SHOW_SERIES_CARDINALITY:
            case SHOW_TAG_KEYS:
            case SHOW_TAG_KEY_CARDINALITY:
            case SHOW_TAG_VALUES:
            case SHOW_TAG_VALUES_CARDINALITY:
                nrPerformed = r.getInteger(0, 1);
                break;
            case DROP_MEASUREMENT:
                if (globalState.usesTSAF()) nrPerformed = r.getInteger(0, 0);
                break;
            default:
                throw new AssertionError(a);
        }
        return nrPerformed;
    }

    @Override
    public SQLConnection createDatabase(InfluxDBGlobalState globalState) throws Exception {
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = InfluxDBOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = InfluxDBOptions.DEFAULT_PORT;
        }
        String bucket = globalState.getDatabaseName();
        InfluxDBAuthorizationParams authorizationParams = JSONObject.parseObject(globalState.getOptions().getParams(),
                InfluxDBAuthorizationParams.class);
        String organizationId = authorizationParams.getOrganizationId();
        String token = authorizationParams.getToken();
        String url = String.format("http://%s:%d/api/v2/buckets", host, port);
        String precision = globalState.getOptions().getPrecision();
        // 暂时仅支持ns
        precision = "ns";
        InfluxDBConnection influxDBConnection = new InfluxDBConnection(host, port, token, organizationId, bucket,
                precision);
        // create database;
        try (InfluxDBStatement s = influxDBConnection.createStatement()) {
            // delete if exist
            // TODO 删除数据库方法存在error
//            String bucketId = deleteDatabaseIfExist(url, bucket, s);
//            globalState.getState().logStatement("QUERY DATABASE " + bucket);
//            if (!StringUtils.isBlank(bucketId))
//                globalState.getState().logStatement("DROP DATABASE " + bucketId);
            globalState.getState().logStatement("CREATE DATABASE " + bucket);

            // create
            JSONObject bucketInfo = new JSONObject();
            bucketInfo.put("orgID", organizationId);
            bucketInfo.put("name", bucket);
            JSONArray retentionRules = new JSONArray();
            JSONObject retentionRule = new JSONObject();
            retentionRule.put("type", "expire");
            retentionRule.put("everySeconds", 0);
            retentionRule.put("shardGroupDurationSeconds", 0);
            retentionRules.add(retentionRule);
            bucketInfo.put("retentionRules", retentionRules);

            s.executeManagerApi(url, bucketInfo.toJSONString(), HttpRequestEnum.POST);
        }
        // TODO
//        if (influxDBConnection.isClosed()) {
//            throw new SQLException("createDatabase error...");
//        }
        return new SQLConnection(influxDBConnection);
    }

    @Override
    public void dropDatabase(InfluxDBGlobalState globalState) throws Exception {

    }

    private String deleteDatabaseIfExist(String baseUrl, String bucketName, InfluxDBStatement s) {
        // get
        String getUrl = String.format("%s?name=%s", baseUrl, bucketName);
        String bucketInfoStr = s.executeManagerApi(getUrl, null, HttpRequestEnum.GET);
        JSONArray bucketsArray = JSONObject.parseObject(bucketInfoStr).getJSONArray("buckets");
        if (bucketsArray.isEmpty()) return "";
        JSONObject bucketInfo = (JSONObject) bucketsArray.get(0);
        String bucketId = bucketInfo.getString("id");

        // delete
        String deleteUrl = String.format("%s/%s", baseUrl, bucketId);
        s.executeManagerApi(deleteUrl, null, HttpRequestEnum.DELETE);
        return bucketId;
    }

    @Override
    public String getDBMSName() {
        return GlobalConstant.INFLUXDB_DATABASE_NAME;
    }


    @Override
    protected void recordQueryExecutionStatistical(InfluxDBGlobalState globalState) {
        // record expression depth
        globalState.getLogger().writeSyntaxErrorQuery(String.format("Expression Iteration Depth:%d",
                InfluxDBQuerySynthesisFeedbackManager.expressionDepth.get()));

        // query execution statistical
        globalState.getLogger().writeSyntaxErrorQuery(String.format("Query statistics:%s",
                InfluxDBQuerySynthesisFeedbackManager.queryExecutionStatistical));

        // query syntax sequence number
        globalState.getLogger().writeSyntaxErrorQuery(String.format("Number of query syntax sequences:%s",
                InfluxDBQuerySynthesisFeedbackManager.querySynthesisFeedback.getSequenceNumber()));

        // query syntax sequence
        globalState.getLogger().writeSyntaxErrorQuery(
                InfluxDBQuerySynthesisFeedbackManager.querySynthesisFeedback.toString());
    }

    @Override
    public void generateDatabase(InfluxDBGlobalState globalState) throws Exception {
        while (globalState.getSchema().getDatabaseTables().size() < Randomly.smallNumber() + 1) {
            String tableName = DBMSCommon.createTableName(globalState.getSchema().getMaxTableIndex() + 1);
            SQLQueryAdapter createTable = InfluxDBTableGenerator.generate(globalState, tableName);
            globalState.executeStatement(createTable);
        }

        StatementExecutor<InfluxDBGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                InfluxDBProvider::mapActions, (q) -> {
            if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                throw new IgnoreMeException();
            }
        });
        se.executeStatements();
    }

    @Override
    public boolean addRowsToAllTables(InfluxDBGlobalState globalState) throws Exception {
        List<InfluxDBSchema.InfluxDBTable> tablesNoRow = globalState.getSchema().getDatabaseTables().stream()
                .filter(t -> t.getNrRows(globalState) == 0).collect(Collectors.toList());
        for (InfluxDBSchema.InfluxDBTable table : tablesNoRow) {
            SQLQueryAdapter queryAddRows = InfluxDBInsertGenerator.insertRow(globalState, table);
            globalState.executeStatement(queryAddRows);
        }
        return true;
    }
}
