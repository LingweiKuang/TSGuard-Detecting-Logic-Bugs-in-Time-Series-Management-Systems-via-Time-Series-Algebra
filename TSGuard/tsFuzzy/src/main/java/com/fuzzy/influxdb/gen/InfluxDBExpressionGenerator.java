package com.fuzzy.influxdb.gen;


import com.fuzzy.Randomly;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.common.gen.UntypedExpressionGenerator;
import com.fuzzy.common.tsaf.EquationsManager;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequency;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;
import com.fuzzy.influxdb.InfluxDBGlobalState;
import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBColumn;
import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBDataType;
import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBRowValue;
import com.fuzzy.influxdb.ast.*;
import com.fuzzy.influxdb.ast.InfluxDBBinaryArithmeticOperation.InfluxDBBinaryArithmeticOperator;
import com.fuzzy.influxdb.ast.InfluxDBBinaryBitwiseOperation.InfluxDBBinaryBitwiseOperator;
import com.fuzzy.influxdb.ast.InfluxDBBinaryComparisonOperation.BinaryComparisonOperator;
import com.fuzzy.influxdb.ast.InfluxDBBinaryLogicalOperation.InfluxDBBinaryLogicalOperator;
import com.fuzzy.influxdb.ast.InfluxDBConstant.InfluxDBDoubleConstant;
import com.fuzzy.influxdb.ast.InfluxDBOrderByTerm.InfluxDBOrder;
import com.fuzzy.influxdb.ast.InfluxDBUnaryNotPrefixOperation.InfluxDBUnaryNotPrefixOperator;
import com.fuzzy.influxdb.ast.InfluxDBUnaryPrefixOperation.InfluxDBUnaryPrefixOperator;
import com.fuzzy.influxdb.feedback.InfluxDBQuerySynthesisFeedbackManager;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
public class InfluxDBExpressionGenerator extends UntypedExpressionGenerator<InfluxDBExpression, InfluxDBColumn> {

    private static final Set<String> pairingProhibited = new HashSet<>();
    private final InfluxDBGlobalState state;
    private InfluxDBRowValue rowVal;

    public InfluxDBExpressionGenerator(InfluxDBGlobalState state) {
        this.state = state;
        if (state.getOptions().useSyntaxValidator()) initGenerator();
    }

    public InfluxDBExpressionGenerator setRowVal(InfluxDBRowValue rowVal) {
        this.rowVal = rowVal;
        return this;
    }

    private void initGenerator() {
        // TODO 查询有效性问题（目前仅靠标记规避 -> 后续应考虑更有效的查询生成方法, 例如基于反馈机制）
        // TODO 重复生成查询问题 -> 记录已生成查询信息, 避免重复生成
        // TODO 表达式节点生成时, 是否应该筛选避免某些节点在Randomly.fromOptions(Actions.values())中出现?
        pairingProhibited.add(genHashKeyWithPairActions(Actions.UNARY_PREFIX_OPERATION, Actions.UNARY_NOT_PREFIX_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.UNARY_PREFIX_OPERATION, Actions.BINARY_LOGICAL_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.UNARY_PREFIX_OPERATION, Actions.BINARY_COMPARISON_OPERATION));

        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_LOGICAL_OPERATOR, Actions.COLUMN));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_LOGICAL_OPERATOR, Actions.LITERAL));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_LOGICAL_OPERATOR, Actions.CAST));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_LOGICAL_OPERATOR, Actions.UNARY_PREFIX_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_LOGICAL_OPERATOR, Actions.BINARY_BITWISE_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_LOGICAL_OPERATOR, Actions.BINARY_ARITHMETIC_OPERATION));
    }

    private enum Actions {
        COLUMN, LITERAL, UNARY_PREFIX_OPERATION, UNARY_NOT_PREFIX_OPERATION,
        BINARY_LOGICAL_OPERATOR, CAST, BINARY_BITWISE_OPERATION, BINARY_ARITHMETIC_OPERATION,
        BINARY_COMPARISON_OPERATION;
    }

    @Override
    public InfluxDBExpression generateExpression(int depth) {
        return null;
    }

    @Override
    protected InfluxDBExpression generateExpression(Object parentActions, int depth) {
        try {
            if (depth >= (state.getOptions().useSyntaxSequence() ?
                    InfluxDBQuerySynthesisFeedbackManager.expressionDepth.get() :
                    state.getOptions().getMaxExpressionDepth())) {
                return generateLeafNode();
            }
            Actions actions = Randomly.fromOptions(Actions.values());
            // 语法序列组合校验
            while (!checkExpressionValidity(parentActions, actions)) {
                actions = Randomly.fromOptions(Actions.values());
            }
            return generateSpecifiedExpression(actions, parentActions, depth);
        } catch (ReGenerateExpressionException e) {
            return generateExpression(parentActions, depth);
        }
    }

    protected InfluxDBExpression generateSpecifiedExpression(Actions actions, Object parentActions, int depth) {
        InfluxDBExpression expression;
        switch (actions) {
            case COLUMN:
                expression = generateColumn();
                break;
            case LITERAL:
                expression = generateConstant();
                break;
            case UNARY_PREFIX_OPERATION:
                expression = new InfluxDBUnaryPrefixOperation(generateExpression(actions, depth + 1),
                        InfluxDBUnaryPrefixOperator.getRandom());
                break;
            case UNARY_NOT_PREFIX_OPERATION:
                InfluxDBExpression subExpr = generateExpression(actions, depth + 1);
                expression = new InfluxDBUnaryNotPrefixOperation(subExpr,
                        InfluxDBUnaryNotPrefixOperator.getRandom(subExpr));
                break;
            case BINARY_LOGICAL_OPERATOR:
                if (depth >= (state.getOptions().useSyntaxSequence() ?
                        InfluxDBQuerySynthesisFeedbackManager.expressionDepth.get() :
                        state.getOptions().getMaxExpressionDepth()) - 1)
                    expression = generateExpression(parentActions, depth);
                else expression = new InfluxDBBinaryLogicalOperation(
                        generateExpression(actions, depth + 1),
                        generateExpression(actions, depth + 1),
                        InfluxDBBinaryLogicalOperator.getRandom());
                break;
            case BINARY_COMPARISON_OPERATION:
                expression = new InfluxDBBinaryComparisonOperation(
                        generateExpression(actions, depth + 1),
                        generateExpression(actions, depth + 1),
                        BinaryComparisonOperator.getRandom());
                break;
            case CAST:
                InfluxDBExpression columnReference = generateColumn();
                expression = new InfluxDBCastOperation(columnReference, InfluxDBCastOperation.CastType.getRandom(
                        ((InfluxDBColumnReference) columnReference).getColumn()));
                break;
            case BINARY_BITWISE_OPERATION:
                // TSQS 暂时不支持按位运算
                if (state.usesTSAF() || state.usesPQS()) expression = ignoreThisExpr(parentActions, actions, depth);
                else expression = new InfluxDBBinaryBitwiseOperation(
                        generateExpression(actions, depth + 1),
                        generateExpression(actions, depth + 1),
                        InfluxDBBinaryBitwiseOperator.getRandom());
                break;
            case BINARY_ARITHMETIC_OPERATION:
                expression = new InfluxDBBinaryArithmeticOperation(
                        generateExpression(actions, depth + 1),
                        generateExpression(actions, depth + 1),
                        InfluxDBBinaryArithmeticOperator.getRandom());
                break;
            default:
                throw new AssertionError();
        }
        if (state.getOptions().useSyntaxValidator()) expression.checkSyntax();
        return expression;
    }

    private boolean checkExpressionValidity(final Object parentActions, final Actions childActions) {
        if (parentActions == null) return true;
        else if (pairingProhibited.contains(
                genHashKeyWithPairActions(Actions.valueOf(parentActions.toString()), childActions))) {
            return false;
        }
        return true;
    }

    @Override
    public InfluxDBExpression generateExpressionForSyntaxValidity(String fatherActions, String childActions) {
        return null;
    }

    private InfluxDBExpression ignoreThisExpr(Object parentActions, Actions action, int depth) {
        return generateExpression(parentActions, depth);
    }

    private String genHashKeyWithPairActions(final Actions parentNode, final Actions childNode) {
        return String.format("%s__%s", parentNode.name(), childNode.name());
    }

    @Override
    public InfluxDBExpression generateConstant() {
        InfluxDBDataType[] values;
        if (state.usesPQS()) {
            values = InfluxDBDataType.valuesPQS();
        } else if (state.usesTSAF()) {
            values = InfluxDBDataType.valuesTSAF();
        } else {
            values = InfluxDBDataType.values();
        }
        switch (Randomly.fromOptions(values)) {
            case INT:
                // Where语句生成常量时不支持后缀解析(u后缀仅支持常量情况, 故将其省略)
                return InfluxDBConstant.createIntConstant(state.getRandomly().getInteger(), true, false);
            case UINT:
                return InfluxDBConstant.createIntConstant(state.getRandomly().getUnsignedInteger(),
                        false, false);
            case STRING:
                String string = state.getRandomly().getString().replace("\\", "").replace("\n", "");
                return InfluxDBConstant.createSingleQuotesStringConstant(string);
            case FLOAT:
                double val = BigDecimal.valueOf(state.getRandomly().getInfiniteDouble()).setScale(
                        InfluxDBDoubleConstant.scale, RoundingMode.HALF_UP).doubleValue();
                return InfluxDBConstant.createDoubleConstant(val);
            case TIMESTAMP:
                return InfluxDBConstant.createIntConstant(System.currentTimeMillis(), /*asBoolean*/false);

            default:
                throw new AssertionError();
        }
    }

    public InfluxDBExpression generateConstantForInfluxDBDataType(InfluxDBDataType influxDBDataType) {
        // TODO
        if (state.usesTSAF()) influxDBDataType = InfluxDBDataType.INT;
        switch (influxDBDataType) {
            case INT:
                return InfluxDBConstant.createIntConstant(state.getRandomly().getInteger());
            case UINT:
                return InfluxDBConstant.createIntConstant(state.getRandomly().getUnsignedInteger(), false);
            // case NULL:
            //     return InfluxDBConstant.createNullConstant();
            case BOOLEAN:
                return InfluxDBConstant.createBoolean(Randomly.getBoolean());
            case STRING:
                String string = state.getRandomly().getString().replace("\\", "").replace("\n", "");
                // 插入数据: influxdb仅支持双引号字符串
                return InfluxDBConstant.createDoubleQuotesStringConstant(string);
            case FLOAT:
                double val = BigDecimal.valueOf(state.getRandomly().getInfiniteDouble()).setScale(
                        InfluxDBDoubleConstant.scale, RoundingMode.HALF_UP).doubleValue();
                return InfluxDBConstant.createDoubleConstant(val);
            default:
                throw new AssertionError();
        }
    }

    @Override
    protected InfluxDBExpression generateColumn() {
        InfluxDBColumn c = Randomly.fromList(columns);
        InfluxDBConstant val;
        if (rowVal == null) {
            // TSQS生成表达式时, rowVal默认值为1, 列存在因子时能够进行区分
            String databaseName = state.getDatabaseName();
            SamplingFrequency samplingFrequency = SamplingFrequencyManager.getInstance()
                    .getSamplingFrequencyFromCollection(databaseName, c.getTable().getName());
            BigDecimal bigDecimal = EquationsManager.getInstance().getEquationsFromTimeSeries(databaseName,
                            c.getTable().getName(), c.getName())
                    .genValueByTimestamp(samplingFrequency, state.getOptions().getStartTimestampOfTSData());
            val = c.getType().isInt() ? InfluxDBConstant.createIntConstant(bigDecimal.longValue()) :
                    c.getType().isNumeric() ? InfluxDBConstant.createBigDecimalConstant(bigDecimal)
                            : InfluxDBConstant.createSingleQuotesStringConstant(bigDecimal.toPlainString());
        } else val = rowVal.getValues().get(c);
        return InfluxDBColumnReference.create(c, val);
    }

    @Override
    public InfluxDBExpression negatePredicate(InfluxDBExpression predicate) {
        return null;
        // return new InfluxDBUnaryPrefixOperation(predicate, InfluxDBUnaryNotPrefixOperator.NOT);
    }

    @Override
    public InfluxDBExpression isNull(InfluxDBExpression expr) {
        return null;
        // return new MySQLUnaryPostfixOperation(expr, MySQLUnaryPostfixOperation.UnaryPostfixOperator.IS_NULL, false);
    }

    @Override
    public List<InfluxDBExpression> generateOrderBys() {
        List<InfluxDBExpression> expressions = new ArrayList<>();
        if (Randomly.getBoolean()) {
            expressions.add(InfluxDBConstant.createDoubleQuotesStringConstant("time"));
        } else {
            expressions.add(InfluxDBConstant.createNullConstant());
        }
        List<InfluxDBExpression> newOrderBys = new ArrayList<>();
        for (InfluxDBExpression expr : expressions) {
            if (Randomly.getBoolean() || expr.getExpectedValue().isNull()) {
                InfluxDBOrderByTerm newExpr = new InfluxDBOrderByTerm(expr, InfluxDBOrder.getRandomOrder());
                newOrderBys.add(newExpr);
            } else {
                newOrderBys.add(expr);
            }
        }
        return newOrderBys;
    }

}
