package com.fuzzy.common.query;

import com.benchmark.commonClass.TSFuzzyStatement;
import com.benchmark.entity.DBValResultSet;
import com.fuzzy.GlobalState;
import com.fuzzy.Main;
import com.fuzzy.SQLConnection;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SQLQueryAdapter extends Query<SQLConnection> {

    private final String query;
    private final ExpectedErrors expectedErrors;
    private final boolean couldAffectSchema;

    public SQLQueryAdapter(String query) {
        this(query, new ExpectedErrors());
    }

    public SQLQueryAdapter(String query, boolean couldAffectSchema) {
        this(query, new ExpectedErrors(), couldAffectSchema);
    }

    public SQLQueryAdapter(String query, ExpectedErrors expectedErrors) {
        this(query, expectedErrors, false);
    }

    public SQLQueryAdapter(String query, ExpectedErrors expectedErrors, boolean couldAffectSchema) {
        this.query = query;
        this.expectedErrors = expectedErrors;
        this.couldAffectSchema = couldAffectSchema;
        checkQueryString();
    }

    private void checkQueryString() {
        if (query.contains("CREATE TABLE") && !query.startsWith("EXPLAIN") && !couldAffectSchema) {
            throw new AssertionError("CREATE TABLE statements should set couldAffectSchema to true");
        }
    }

    @Override
    public String getQueryString() {
        return query;
    }

    public String getUnterminatedAndNoQEqualQueryString() {
        String result;
        if (query.endsWith(";")) {
            result = query.substring(0, query.length() - 1);
        } else {
            result = query;
        }
        if (query.startsWith("q=")) result = result.substring(2);
        assert !result.endsWith(";");
        assert !result.startsWith("q=");
        return result;
    }

    @Override
    public String getUnterminatedQueryString() {
        String result;
        if (query.endsWith(";")) {
            result = query.substring(0, query.length() - 1);
        } else {
            result = query;
        }
        assert !result.endsWith(";");
        return result;
    }

    @Override
    public <G extends GlobalState<?, ?, SQLConnection>> boolean execute(G globalState, String... fills)
            throws SQLException {
        TSFuzzyStatement statement;
        if (fills.length > 0) {
            // TODO
            statement = globalState.getConnection().prepareStatement(fills[0]);
            for (int i = 1; i < fills.length; i++) {
                ((PreparedStatement) statement).setString(i, fills[i]);
            }
        } else {
            statement = globalState.getConnection().createStatement();
        }
        try {
            if (fills.length > 0) {
                ((PreparedStatement) statement).execute();
            } else {
                statement.execute(query);
            }
            Main.nrSuccessfulActions.addAndGet(1);
            return true;
        } catch (Exception e) {
            Main.nrUnsuccessfulActions.addAndGet(1);
            String expectedException = checkException(e);
            globalState.getLogger().writeSyntaxErrorQuery(String.format("Expected SQL error: query:%s expectedException: %s",
                    query, expectedException));
            return false;
        } finally {
            statement.close();
        }
    }

    public String checkException(Exception e) throws AssertionError {
        Throwable ex = e;

        while (ex != null) {
            if (expectedErrors.errorIsExpected(ex.getMessage())) {
                return ex.getMessage();
            } else {
                ex = ex.getCause();
            }
        }

        throw new AssertionError(query, e);
    }

    @Override
    public <G extends GlobalState<?, ?, SQLConnection>> DBValResultSet executeAndGet(G globalState, String... fills)
            throws SQLException {
        TSFuzzyStatement s;
        if (fills.length > 0) {
            s = globalState.getConnection().prepareStatement(fills[0]);
            for (int i = 1; i < fills.length; i++) {
                ((PreparedStatement) s).setString(i, fills[i]);
            }
        } else {
            s = globalState.getConnection().createStatement();
        }
        DBValResultSet result = null;
        try {
            if (fills.length > 0) {
                // result = ((PreparedStatement) s).executeQuery();
            } else {
                result = s.executeQuery(query);
            }
            Main.nrSuccessfulActions.addAndGet(1);
            if (result == null) {
                return null;
            }
            return result;
        } catch (Exception e) {
            s.close();
            Main.nrUnsuccessfulActions.addAndGet(1);
            String expectedException = checkException(e);
            globalState.getLogger().writeSyntaxErrorQuery(String.format("Expected SQL error: %s", expectedException));
        }
        return null;
    }

    @Override
    public boolean couldAffectSchema() {
        return couldAffectSchema;
    }

    @Override
    public ExpectedErrors getExpectedErrors() {
        return expectedErrors;
    }

    @Override
    public String getLogString() {
        return getQueryString();
    }
}
