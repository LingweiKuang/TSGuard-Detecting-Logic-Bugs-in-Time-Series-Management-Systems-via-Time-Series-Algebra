package com.fuzzy.common.query;

@FunctionalInterface
public interface SQLQueryProvider<S> {
    SQLQueryAdapter getQuery(S globalState) throws Exception;
}
