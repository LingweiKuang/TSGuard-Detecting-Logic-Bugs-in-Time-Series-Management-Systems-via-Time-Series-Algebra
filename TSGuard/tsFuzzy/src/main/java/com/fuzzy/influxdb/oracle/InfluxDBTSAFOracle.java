package com.fuzzy.influxdb.oracle;


import com.benchmark.entity.DBValResultSet;
import com.fuzzy.Randomly;
import com.fuzzy.SQLConnection;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.common.oracle.TimeSeriesAlgebraFrameworkBase;
import com.fuzzy.common.query.Query;
import com.fuzzy.common.query.QueryExecutionStatistical;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.common.schema.AbstractTableColumn;
import com.fuzzy.common.tsaf.PredicationEquation;
import com.fuzzy.common.tsaf.QueryType;
import com.fuzzy.common.tsaf.TableToNullValuesManager;
import com.fuzzy.common.tsaf.TimeSeriesConstraint;
import com.fuzzy.influxdb.InfluxDBErrors;
import com.fuzzy.influxdb.InfluxDBGlobalState;
import com.fuzzy.influxdb.InfluxDBSchema;
import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBColumn;
import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBDataType;
import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBRowValue;
import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBTable;
import com.fuzzy.influxdb.InfluxDBVisitor;
import com.fuzzy.influxdb.ast.*;
import com.fuzzy.influxdb.feedback.InfluxDBQuerySynthesisFeedbackManager;
import com.fuzzy.influxdb.gen.InfluxDBExpressionGenerator;
import com.fuzzy.influxdb.gen.InfluxDBInsertGenerator;
import com.fuzzy.influxdb.gen.InfluxDBTimeExpressionGenerator;
import com.fuzzy.influxdb.resultSet.InfluxDBResultSet;
import com.fuzzy.influxdb.resultSet.InfluxDBSeries;
import com.fuzzy.influxdb.tsaf.InfluxDBAggregationType;
import com.fuzzy.influxdb.tsaf.InfluxDBTimeSeriesFunc;
import com.fuzzy.influxdb.util.InfluxDBValueStateConstant;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public abstract class InfluxDBTSAFOracle
        extends TimeSeriesAlgebraFrameworkBase<InfluxDBGlobalState, InfluxDBRowValue, InfluxDBExpression, SQLConnection> {

    // TODO 插入数据空间概率分布
    // 数值和时间戳具备二元函数关系（能否知微见著，反映出各种不规律数值空间？）
    // 预期结果集由两者得出
    // TODO 完整插入所有数据后，随机更新某些时间戳数据（或者插入数据时间戳乱序，前者验证lsm树更新操作，后者验证时序数据库的合并排序）
    // TODO （时间戳和数值按列单独存储，所以两者之间的关系性不会影响到时序数据库查询？看目前是否有时序数据库压缩，查询操作考虑到多列相关性）
    private List<InfluxDBExpression> fetchColumns;
    private List<InfluxDBColumn> columns;
    private InfluxDBTable table;
    private InfluxDBExpression whereClause;
    InfluxDBSelect selectStatement;

    public InfluxDBTSAFOracle(InfluxDBGlobalState globalState) {
        super(globalState);
        InfluxDBErrors.addExpressionErrors(errors);
    }

    @Override
    public Query<SQLConnection> getTimeSeriesQuery() {
        selectStatement = new InfluxDBSelect();
        InfluxDBSchema.InfluxDBTables randomFromTables = globalState.getSchema().getRandomTableNonEmptyTables();
        table = randomFromTables.getTables().get(0);
        selectStatement.setFromList(Collections.singletonList(new InfluxDBTableReference(table)));
        // 注: TSAF和大多数时序数据库均不支持将time字段和其他字段进行比较、算术二元运算
        columns = table.getColumns().stream().filter(c -> !c.isTag()).collect(Collectors.toList());

//        QueryType queryType = Randomly.fromOptions(QueryType.values());
        QueryType queryType = Randomly.fromOptions(QueryType.BASE_QUERY);
        selectStatement.setQueryType(queryType);
        // 随机窗口查询测试
        if (queryType.isTimeWindowQuery()) generateTimeWindowClause();
        else if (queryType.isTimeSeriesFunction())
            selectStatement.setTimeSeriesFunction(InfluxDBTimeSeriesFunc.getRandomFunction(
                    columns, globalState.getRandomly()));
        fetchColumns = columns.stream().map(column -> {
            String columnName = column.getName();
            if (queryType.isTimeWindowQuery())
                columnName = String.format("%s(%s)",
                        InfluxDBAggregationType.getInfluxDBAggregationType(selectStatement.getAggregationType()), columnName);
            else if (queryType.isTimeSeriesFunction())
                columnName = selectStatement.getTimeSeriesFunction().combinedArgs(columnName);
            return new InfluxDBColumnReference(new InfluxDBColumn(columnName, column.isTag(), column.getType()), null);
        }).collect(Collectors.toList());
        selectStatement.setFetchColumns(fetchColumns);
        whereClause = generateExpression(columns);
        selectStatement.setWhereClause(whereClause);

        return new SQLQueryAdapter(InfluxDBVisitor.asString(selectStatement), errors);
    }



    @Override
    protected void setSequenceRegenerateProbabilityToMax(String sequence) {
        InfluxDBQuerySynthesisFeedbackManager.setSequenceRegenerateProbabilityToMax(sequence);
    }

    @Override
    protected void incrementQueryExecutionCounter(QueryExecutionStatistical.QueryExecutionType queryType) {
        switch (queryType) {
            case error:
                InfluxDBQuerySynthesisFeedbackManager.incrementErrorQueryCount();
                break;
            case invalid:
                InfluxDBQuerySynthesisFeedbackManager.incrementInvalidQueryCount();
                break;
            case success:
                InfluxDBQuerySynthesisFeedbackManager.incrementSuccessQueryCount();
                break;
            default:
                throw new UnsupportedOperationException(String.format("Invalid QueryExecutionType: %s", queryType));
        }
    }

    @Override
    protected TimeSeriesConstraint genColumnConstraint(InfluxDBExpression expr) {
        return InfluxDBVisitor.asConstraint(globalState.getDatabaseName(), table.getName(), expr,
                TableToNullValuesManager.getNullValues(globalState.getDatabaseName(), table.getName()));
    }

    @Override
    protected Map<Long, List<BigDecimal>> getExpectedValues(InfluxDBExpression expression) {
        // 联立方程式求解
        String databaseName = globalState.getDatabaseName();
        String tableName = table.getName();
        List<String> fetchColumnNames = columns.stream().map(AbstractTableColumn::getName).collect(Collectors.toList());
        TimeSeriesConstraint timeSeriesConstraint = InfluxDBVisitor.asConstraint(databaseName, tableName,
                expression, TableToNullValuesManager.getNullValues(databaseName, tableName));
        PredicationEquation predicationEquation = new PredicationEquation(timeSeriesConstraint, "equationName");
        return predicationEquation.genExpectedResultSet(databaseName, tableName, fetchColumnNames,
                globalState.getOptions().getStartTimestampOfTSData(),
                InfluxDBInsertGenerator.getLastTimestamp(databaseName, tableName),
                InfluxDBConstant.createFloatArithmeticTolerance());
    }

    @Override
    protected boolean verifyResultSet(Map<Long, List<BigDecimal>> expectedResultSet, DBValResultSet result) {
        try {
            if (selectStatement.getQueryType().isTimeSeriesFunction()
                    || selectStatement.getQueryType().isTimeWindowQuery())
                return verifyTimeWindowQuery(expectedResultSet, result);
            else return verifyGeneralQuery(expectedResultSet, result);
        } catch (Exception e) {
            log.error("Verify: RESULT SET &  EXPECTED RESULT are the same., e:", e);
            return false;
        }
    }

    private boolean verifyTimeWindowQuery(Map<Long, List<BigDecimal>> expectedResultSet, DBValResultSet result)
            throws Exception {
        // 对expectedResultSet进行聚合, 聚合结果按照时间戳作为Key, 进行进一步数值比较
        InfluxDBResultSet influxDBResultSet = (InfluxDBResultSet) result;
        Map<Long, List<BigDecimal>> resultSet = null;
        if (!result.hasNext()) {
            if (selectStatement.getQueryType().isTimeWindowQuery()) return expectedResultSet.isEmpty();
            else
                return InfluxDBTimeSeriesFunc.apply(selectStatement.getTimeSeriesFunction(), expectedResultSet).isEmpty();
        }

        // 窗口聚合 -> 预期结果集
        if (selectStatement.getQueryType().isTimeWindowQuery()) {
            InfluxDBColumn timeColumn = new InfluxDBColumn(InfluxDBValueStateConstant.TIME_FIELD.getValue(),
                    false, InfluxDBDataType.TIMESTAMP);
            int timeIndex = influxDBResultSet.findColumn(timeColumn.getName());
            // 获取窗口划分初始时间戳
            long startTimestamp = getConstantFromResultSet(influxDBResultSet.getCurrentValue().getValues().get(0),
                    timeColumn.getType(), timeIndex).getBigDecimalValue().longValue();
            String intervalVal = selectStatement.getIntervalValues().get(0);
            long duration = globalState.transTimestampToPrecision(
                    Long.parseLong(intervalVal.substring(0, intervalVal.length() - 1)) * 1000);
            resultSet = selectStatement.getAggregationType()
                    .apply(expectedResultSet, startTimestamp, duration, duration);
        } else if (selectStatement.getQueryType().isTimeSeriesFunction())
            resultSet = InfluxDBTimeSeriesFunc.apply(selectStatement.getTimeSeriesFunction(), expectedResultSet);

        // 验证结果
        result.resetCursor();
        boolean verifyRes = verifyGeneralQuery(resultSet, result);
        if (!verifyRes) logAggregationSet(resultSet);
        return verifyRes;
    }

    private boolean verifyGeneralQuery(Map<Long, List<BigDecimal>> expectedResultSet, DBValResultSet result)
            throws Exception {
        // 针对每行进行验证
        while (result.hasNext()) {
            if (verifyRowResult(expectedResultSet, result) == VerifyResultState.FAIL) return false;
        }

        return true;
    }

    private VerifyResultState verifyRowResult(Map<Long, List<BigDecimal>> expectedResultSet, DBValResultSet result)
            throws Exception {
        getExpectedValues(whereClause);
        InfluxDBResultSet influxDBResultSet = (InfluxDBResultSet) result;
        InfluxDBColumn timeColumn = new InfluxDBColumn(InfluxDBValueStateConstant.TIME_FIELD.getValue(),
                false, InfluxDBDataType.TIMESTAMP);
        int timeIndex = influxDBResultSet.findColumn(timeColumn.getName());

        for (int i = 0; i < fetchColumns.size(); i++) {
            // 评估整条时间序列
            InfluxDBColumn dataColumn = ((InfluxDBColumnReference) fetchColumns.get(i)).getColumn();

            // 聚合函数列名取聚合函数名
            String columnName = InfluxDBValueStateConstant.REF.getValue() + i;
            int columnIndex = influxDBResultSet.findColumn(columnName);
            InfluxDBSeries currentValue = influxDBResultSet.getCurrentValue();
            int nullValueCount = 0;
            for (List<String> stringValues : currentValue.getValues()) {
                InfluxDBConstant dataConstant = getConstantFromResultSet(stringValues, dataColumn.getType(), columnIndex);
                if (dataConstant.isNull()) {
                    nullValueCount++;
                    continue;
                }
                long timestamp = getConstantFromResultSet(stringValues, timeColumn.getType(), timeIndex)
                        .getBigDecimalValue().longValue();

                if (!expectedResultSet.containsKey(timestamp)) {
                    log.error("Expected result NOT contain the ACTUAL result timestamp, timestamp:{}", timestamp);
                    return VerifyResultState.FAIL;
                }
                InfluxDBConstant isEquals = dataConstant.isEquals(
                        new InfluxDBConstant.InfluxDBBigDecimalConstant(expectedResultSet.get(timestamp).get(i)));
                if (isEquals.isNull()) throw new AssertionError();
                else if (!isEquals.asBooleanNotNull()) {
                    log.error("Data anomalies at specific timestamps, timestamp:{}, expectedValue:{}, actualValue:{}",
                            timestamp, expectedResultSet.get(timestamp), dataConstant.getBigDecimalValue());
                    return VerifyResultState.FAIL;
                }
            }
            // 数目
            if (expectedResultSet.size() != currentValue.getValues().size() - nullValueCount) {
                log.error("EXPECTED result and the ACTUAL result were different., expectedResultSetSize:{} rowCount:{} nullValueCount:{}",
                        expectedResultSet.size(), currentValue.getValues().size(), nullValueCount);
                return VerifyResultState.FAIL;
            }
        }
        return VerifyResultState.SUCCESS;
    }

    protected abstract boolean containsRows(TimeSeriesConstraint constraint);

    private enum VerifyResultState {
        SUCCESS, FAIL, IS_NULL
    }

    private void logAggregationSet(Map<Long, List<BigDecimal>> aggregationResultSet) {
        globalState.getState().getLocalState().log(String.format("aggregationResultSet size:%d %s",
                aggregationResultSet.size(), aggregationResultSet));
    }

    private InfluxDBConstant getConstantFromResultSet(List<String> resultSet, InfluxDBDataType dataType,
                                                      int columnIndex) {
        if (resultSet.get(columnIndex).equalsIgnoreCase("null")) return InfluxDBConstant.createNullConstant();

        InfluxDBConstant constant;
        switch (dataType) {
            case BOOLEAN:
                constant = InfluxDBConstant.createBoolean(
                        resultSet.get(columnIndex).equalsIgnoreCase("true"));
                break;
            case STRING:
                constant = InfluxDBConstant.createSingleQuotesStringConstant(resultSet.get(columnIndex));
                break;
            case INT:
            case UINT:
            case FLOAT:
            case BIGDECIMAL:
                constant = InfluxDBConstant.createBigDecimalConstant(
                        new BigDecimal(resultSet.get(columnIndex)));
                break;
            case TIMESTAMP:
                // 时间格式 -> 直接返回, 不再进行空值判定
                constant = InfluxDBConstant.createBigDecimalConstant(
                        new BigDecimal(globalState.transDateToTimestamp(resultSet.get(columnIndex))));
                break;
            default:
                throw new AssertionError(dataType);
        }
        if (selectStatement.getAggregationType() != null
                && InfluxDBAggregationType.getInfluxDBAggregationType(selectStatement.getAggregationType()).isFillZero()
                && constant.getBigDecimalValue().compareTo(BigDecimal.ZERO) == 0
                && dataType != InfluxDBDataType.TIMESTAMP)
            return InfluxDBConstant.createNullConstant();
        return constant;
    }

    private InfluxDBExpression generateExpression(List<InfluxDBColumn> columns) {
        InfluxDBExpression result = null;
        boolean reGenerateExpr;
        int regenerateCounter = 0;
        String predicateSequence = "";
        do {
            reGenerateExpr = false;
            try {
                InfluxDBExpression predicateExpression = new InfluxDBExpressionGenerator(globalState).setColumns(columns)
                        .generateExpression();
                // 将表达式纠正为BOOLEAN类型
                InfluxDBExpression rectifiedPredicateExpression = predicateExpression;
                if (!predicateExpression.getExpectedValue().isBoolean()) {
                    rectifiedPredicateExpression =
                            InfluxDBUnaryNotPrefixOperation.getNotUnaryPrefixOperation(predicateExpression);
                }

                // generate time column
                InfluxDBColumn timeColumn = new InfluxDBColumn(InfluxDBValueStateConstant.TIME_FIELD.getValue(),
                        false, InfluxDBDataType.INT);

//                InfluxDBTimeExpressionGenerator timeExpressionGenerator = new InfluxDBTimeExpressionGenerator(globalState);
//                InfluxDBExpression timeExpression = timeExpressionGenerator.setColumns(
//                        Collections.singletonList(timeColumn)).generateExpression();
//
//                result = new InfluxDBBinaryLogicalOperation(rectifiedPredicateExpression, timeExpression,
//                        InfluxDBBinaryLogicalOperation.InfluxDBBinaryLogicalOperator.AND);

                //fix the time expression generator
                InfluxDBTimeExpressionGenerator timeExpressionGenerator = new InfluxDBTimeExpressionGenerator(globalState);
                InfluxDBExpression timeExpression = timeExpressionGenerator.setColumns(Collections.singletonList(timeColumn)).generateExpression();

                if (timeExpression.getExpectedValue().isBoolean()){
                    result = new InfluxDBBinaryLogicalOperation(
                            rectifiedPredicateExpression,
                            timeExpression,
                            InfluxDBBinaryLogicalOperation.InfluxDBBinaryLogicalOperator.AND);
                } else {
                    result = rectifiedPredicateExpression;
                }

                String expressionStr = InfluxDBVisitor.asString(result);
                log.info("Expression: {}", expressionStr);

                // 语法节点序列指导查询合成
                predicateSequence = InfluxDBVisitor.asString(rectifiedPredicateExpression, true);
                if (globalState.getOptions().useSyntaxSequence()
                        && InfluxDBQuerySynthesisFeedbackManager.isRegenerateSequence(predicateSequence)) {
                    regenerateCounter++;
                    if (regenerateCounter >= InfluxDBQuerySynthesisFeedbackManager.MAX_REGENERATE_COUNT_PER_ROUND) {
                        InfluxDBQuerySynthesisFeedbackManager.incrementExpressionDepth(
                                globalState.getOptions().getMaxExpressionDepth());
                        regenerateCounter = 0;
                    }
                    throw new ReGenerateExpressionException(String.format("The syntax node sequence needs to be regenerated: %s", predicateSequence));
                }
                // 更新概率表
                InfluxDBQuerySynthesisFeedbackManager.addSequenceRegenerateProbability(predicateSequence);
            } catch (ReGenerateExpressionException e) {
                log.info("ReGenerateExpression: {}", e.getMessage());
                reGenerateExpr = true;
            }
        } while (reGenerateExpr);

        timePredicate = result;
        this.predicateSequence = predicateSequence;
        return result;
    }

    private void generateTimeWindowClause() {
        selectStatement.setAggregationType(InfluxDBAggregationType.getRandomAggregationType());

        List<String> intervals = new ArrayList<>();
        // TODO 采样点前后1000s 取决于采样点endStartTimestamp
        // TODO 时间范围
        long timestampInterval = 1000 * 1000000L * 1000;
        String timeUnit = "s";
        long windowSize = globalState.getRandomly().getLong(100000, timestampInterval / 1000);
        // TODO OFFSETSIZE 数值应用后返回大量无效值
//        long offsetSize = globalState.getRandomly().getLong(100000, timestampInterval / 1000);
        intervals.add(String.format(String.format("%d%s", windowSize, timeUnit)));
//        intervals.add(String.format(String.format("%d%s", offsetSize, timeUnit)));
        selectStatement.setIntervalValues(intervals);
    }

    private List<InfluxDBExpression> generateGroupByClause(List<InfluxDBColumn> columns, InfluxDBRowValue rw) {
        if (Randomly.getBoolean()) {
            return columns.stream().map(c -> InfluxDBColumnReference.create(c, rw.getValues().get(c)))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }
}
