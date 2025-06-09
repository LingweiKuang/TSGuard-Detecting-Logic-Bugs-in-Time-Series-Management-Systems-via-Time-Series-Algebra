package com.fuzzy.common.oracle;


import cn.hutool.core.collection.ConcurrentHashSet;
import com.alibaba.fastjson.JSONObject;
import com.benchmark.entity.DBValResultSet;
import com.fuzzy.GlobalState;
import com.fuzzy.IgnoreMeException;
import com.fuzzy.TSFuzzyDBConnection;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.Query;
import com.fuzzy.common.query.QueryExecutionStatistical;
import com.fuzzy.common.schema.AbstractRowValue;
import com.fuzzy.common.tsaf.TimeSeriesConstraint;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Slf4j
public abstract class TimeSeriesAlgebraFrameworkBase<S extends GlobalState<?, ?, C>, R extends AbstractRowValue<?, ?, ?>, E, C extends TSFuzzyDBConnection>
        implements TestOracle<S> {

    protected final ExpectedErrors errors = new ExpectedErrors();

    // 时间谓词 -> 筛选数据
    protected E timePredicate = null;
    protected String predicateSequence = "";
    protected static final ConcurrentHashSet<String> pivotRowAbstractPredicates = new ConcurrentHashSet<>();

    protected final S globalState;

    protected TimeSeriesAlgebraFrameworkBase(S globalState) {
        this.globalState = globalState;
    }

    @Override
    public void check() throws Exception {
        Query<C> timeseriesQuery = getTimeSeriesQuery();
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(timeseriesQuery.getQueryString());
        }

        boolean pivotRowIsContained = containsRows(timeseriesQuery);
        if (!pivotRowIsContained) {
            reportMissingPivotRow(timeseriesQuery);
        } else {
            // statistical
            incrementQueryExecutionCounter(QueryExecutionStatistical.QueryExecutionType.success);
            log.info("Valid query: {}", timeseriesQuery.getQueryString());
        }
    }

    private boolean containsRows(Query<C> query) throws Exception {
        // 验证预期结果集和实际结果集等价性
        try (DBValResultSet result = query.executeAndGet(globalState)) {
            if (result == null) {
                globalState.getLogger().writeSyntaxErrorQuery(String.format("Invalid query: %s;",
                        query.getQueryString()));
                // 将无效查询序列重生成概率设为10
                setSequenceRegenerateProbabilityToMax(predicateSequence);
                // statistical
                incrementQueryExecutionCounter(QueryExecutionStatistical.QueryExecutionType.invalid);
                throw new IgnoreMeException();
            }

            // 列方程式获取预期结果集
            Map<Long, List<BigDecimal>> expectedResultSet = null;
            try {
                expectedResultSet = getExpectedValues(timePredicate);
            } catch (IgnoreMeException e) {
                // TODO TSAF 暂不支持该复杂表达式运算, 将其命中统计为success
                incrementQueryExecutionCounter(QueryExecutionStatistical.QueryExecutionType.success);
                throw e;
            }
            // 验证结果集
            return verifyResultSet(expectedResultSet, result);
        }
    }

    protected void reportMissingPivotRow(Query<?> query) {
        // statistical
        incrementQueryExecutionCounter(QueryExecutionStatistical.QueryExecutionType.error);

        StringBuilder sb = new StringBuilder();
        if (timePredicate != null) {
            Map<Long, List<BigDecimal>> expectedValues = getExpectedValues(timePredicate);
            sb.append("--\n-- time predicates and their expected values:\n")
                    .append("-- size: ")
                    .append(expectedValues.size())
                    .append("-- ")
                    .append(expectedValues.toString().replace("\n", "\n-- "));
            TimeSeriesConstraint constraint = genColumnConstraint(timePredicate);
            sb.append("--\n-- column constraint:\n")
                    .append("-- ")
                    .append(JSONObject.toJSONString(constraint));
            // 表达式抽象结构
            pivotRowAbstractPredicates.forEach(pivotRow -> {
                if (pivotRow.contains(predicateSequence)) {
                    // 探索空间：表达式递归深度逐级递增
                    // => 从小序列搜集到大序列, 但凡存在包含关系的pivotRowAbstractPredicates均忽略重复报错
                    System.out.println("Sequence error repeat");
                }
            });
            sb.append("--\n-- time predicates abstract expression:\n")
                    .append(predicateSequence)
                    .append("\n--");
            pivotRowAbstractPredicates.add(predicateSequence);
            // 将出错序列重生成概率设为10
            setSequenceRegenerateProbabilityToMax(predicateSequence);
        }
        globalState.getState().getLocalState().log(sb.toString());
        throw new AssertionError(query);
    }

    protected abstract Query<C> getTimeSeriesQuery() throws Exception;

    protected abstract void setSequenceRegenerateProbabilityToMax(String sequence);

    protected abstract void incrementQueryExecutionCounter(QueryExecutionStatistical.QueryExecutionType queryType);

    protected abstract TimeSeriesConstraint genColumnConstraint(E expr);

    protected abstract Map<Long, List<BigDecimal>> getExpectedValues(E expr);

    protected abstract boolean verifyResultSet(Map<Long, List<BigDecimal>> expectedResultSet, DBValResultSet result);

}
