package com.fuzzy.common.gen;


import com.fuzzy.Randomly;

import java.util.ArrayList;
import java.util.List;

public abstract class UntypedExpressionGenerator<E, C> implements ExpressionGenerator<E> {

    protected List<C> columns;
    protected boolean allowAggregates;

    public E generateExpression() {
//        return generateExpression(0);
        return generateExpression(null, 0);
    }

    public abstract E generateConstant();

    protected abstract E generateExpression(int depth);

    protected abstract E generateExpression(Object parentActions, int depth);

    public abstract E generateExpressionForSyntaxValidity(String fatherActions, String childActions);

    protected abstract E generateColumn();

    @SuppressWarnings("unchecked") // unsafe
    public <U extends UntypedExpressionGenerator<E, C>> U setColumns(List<C> columns) {
        this.columns = columns;
        return (U) this;
    }

    public E generateLeafNode() {
        if (Randomly.getBoolean() && !columns.isEmpty()) {
            return generateColumn();
        } else {
            return generateConstant();
        }
    }

    public List<E> generateExpressions(int nr) {
        List<E> expressions = new ArrayList<>();
        for (int i = 0; i < nr; i++) {
            expressions.add(generateExpression());
        }
        return expressions;
    }

    public List<E> generateExpressions(int nr, int depth) {
        List<E> expressions = new ArrayList<>();
        for (int i = 0; i < nr; i++) {
            expressions.add(generateExpression(depth));
        }
        return expressions;
    }

    // override this class to also dropRandomTable ASC, DESC
    public List<E> generateOrderBys() {
        return generateExpressions(Randomly.smallNumber() + 1);
    }

    // override this class to dropRandomTable aggregate functions
    public E generateHavingClause() {
        allowAggregates = true;
        E expr = generateExpression();
        allowAggregates = false;
        return expr;
    }

    @Override
    public E generatePredicate() {
        return generateExpression();
    }

}
