package com.fuzzy.influxdb.oracle;

import com.benchmark.entity.DBVal;
import com.benchmark.entity.DBValResultSet;
import com.benchmark.influxdb.hint.ExpectedResultGenerator;
import com.benchmark.influxdb.hint.FluxHintInjector;
import com.benchmark.influxdb.hint.HintGenerator;
import com.benchmark.influxdb.hint.ResultComparator;
import com.fuzzy.GlobalState;
import com.fuzzy.Reproducer;
import com.fuzzy.SQLConnection;
import com.fuzzy.common.oracle.TestOracle;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.Query;
import com.fuzzy.common.query.QueryExecutionStatistical;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.common.tsaf.TimeSeriesConstraint;
import com.fuzzy.influxdb.InfluxDBErrors;
import com.fuzzy.influxdb.InfluxDBGlobalState;
import com.fuzzy.influxdb.ast.InfluxDBExpression;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class InfluxDBHintOracle implements TestOracle<InfluxDBGlobalState> {

    private final InfluxDBGlobalState globalState;
    private final HintGenerator hintGen = new HintGenerator();
    //private static final Path CENTRAL_HINT_LOG = Paths.get("logs","hint-mismatches.log");

    public InfluxDBHintOracle(InfluxDBGlobalState state) {
        this.globalState = state;
    }

    public static final AtomicInteger nrHintQueries = new AtomicInteger();
    public static final AtomicInteger nrHintMismatches = new AtomicInteger();

    private static final Path CENTRAL_HINT_LOG = Paths.get("logs","hint-mismatches.log");
    static {
        try {
            Files.createDirectories(CENTRAL_HINT_LOG.getParent());
            if (! Files.exists(CENTRAL_HINT_LOG)) {
                Files.createFile(CENTRAL_HINT_LOG);
            }
        } catch (IOException e) {
            log.error("Could not initialize central hint log", e);
        }
    }



    @Override
    public void check() throws Exception {
        //test
        String summary = null;



        InfluxDBTSAFOracle tsafOracle = new InfluxDBTSAFOracle(globalState) {
            {
                InfluxDBErrors.addExpressionErrors(errors);
            }
            @Override
            protected void setSequenceRegenerateProbabilityToMax(String sequence) {
            }
            @Override
            protected void incrementQueryExecutionCounter(QueryExecutionStatistical.QueryExecutionType t) {
            }
            @Override
            protected com.fuzzy.common.tsaf.TimeSeriesConstraint genColumnConstraint(
                    com.fuzzy.influxdb.ast.InfluxDBExpression expr) {
                return null; // not used here
            }
            @Override
            protected boolean verifyResultSet(
                    java.util.Map<Long, java.util.List<java.math.BigDecimal>> expected,
                    DBValResultSet actual) {
                return true;
            }

            @Override
            protected boolean containsRows(TimeSeriesConstraint constraint) {
                return false;
            }
        };


        Query<SQLConnection> baseQuery;
        while (true) {
            try {
                baseQuery = tsafOracle.getTimeSeriesQuery();
                break;
            } catch (AssertionError ae) {
            }
        }




//        globalState.getLogger()
//                .writeCurrent("-- HINT begin bucket “" + globalState.getDatabaseName() + "”");
//
//        Query<SQLConnection> baseQuery;
//        try{
//            baseQuery = tsafOracle.getTimeSeriesQuery();
//        } catch (Throwable t){
//            globalState.getLogger()
//                    .writeCurrent("Hint‐Based: SKIP bucket “" + globalState.getDatabaseName()
//                            + "” (no TSAL query: " + t.getClass().getSimpleName() + ")");
//            return;
//        }

        String baseQueryString = baseQuery.getQueryString(); // like "q=<flux>…"
        if (!baseQueryString.startsWith("q=")) {
            return;
        }

        globalState.getLogger().writeCurrent(baseQueryString);

        String baseFlux = baseQueryString.substring(2);

        ExpectedErrors noErrors = new ExpectedErrors();
        DBValResultSet baseRs;
        try {
            baseRs = globalState.getManager()
                    .executeAndGet(new SQLQueryAdapter(baseQueryString, noErrors));
        } catch (Exception e) {
            globalState.getLogger().writeSyntaxErrorQuery("Invalid query: " + baseQueryString + ";");
            return;
        }
        List<DBVal> baseline = baseRs.getDBVals();

        String hint = hintGen.nextHint();

        //inject hint
        String hintedFlux = FluxHintInjector.applyHint(baseFlux, hint);
        String hintedQueryString = "q=" + hintedFlux;

        //log query with hint
        globalState.getLogger().writeCurrent(hintedQueryString);

        DBValResultSet actualRs;
        try {
            actualRs = globalState.getManager()
                    .executeAndGet(new SQLQueryAdapter(hintedQueryString, noErrors));
        } catch (Exception e) {
            // Syntax error in hintedFlux → record and skip
            globalState.getLogger().writeSyntaxErrorQuery("Invalid query (hinted): " + hintedQueryString + ";");
            return;
        }
        List<DBVal> actual = actualRs.getDBVals();

        //get expected result
        List<DBVal> expected;
        try {
            expected = ExpectedResultGenerator.apply(baseline, hint);
        } catch (NullPointerException npe) {
            return;
        }

        //compare
        List<String> mismatches = Collections.emptyList();
        try {
            mismatches = ResultComparator.compare(expected, actual);
        } catch (Exception compareErr) {
            return;
        }

        if (mismatches.isEmpty()){
            globalState.getLogger()
                    .writeCurrent("Hint‐Based: no mismatches found in bucket “"
                            + globalState.getDatabaseName() + "”");
        }

        if (!mismatches.isEmpty()) {
            log.error("Bucket “{}” – hint “{}” produced unexpected results:",
                    globalState.getDatabaseName(), hint);
            mismatches.forEach(m -> log.error("  • " + m));

            // Build a single‐line summary, then append to the central log file
            String line = String.format(
                    "%s  hint=<%s>  mismatches=%s",
                    globalState.getDatabaseName(),
                    hint,
                    mismatches.toString()
            );
            try {
                Files.write(
                        CENTRAL_HINT_LOG,
                        Collections.singletonList(line),
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND
                );
            } catch (IOException ioe) {
                log.error("Could not write to hint-mismatches.log", ioe);
            }

            throw new AssertionError("Flux hint mismatch in bucket “"
                    + globalState.getDatabaseName() + "”");
        }

        try {
            summary = mismatches.isEmpty()
                    ? "Hint-Based: no mismatches in “" + globalState.getDatabaseName() + "”"
                    : "Hint-Based: mismatches in “" + globalState.getDatabaseName() + "” → " + mismatches;
            if (!mismatches.isEmpty()) {
                throw new AssertionError("Flux hint mismatch");
            }
        } catch (Throwable t) {
            // optional: capture exception as part of summary
            if (summary == null) {
                summary = "Hint-Based: failed on “"
                        + globalState.getDatabaseName() + "” – "
                        + t.getClass().getSimpleName()
                        + (t.getMessage()==null ? "" : ": "+t.getMessage());
            }
        } finally {
            globalState.getLogger().writeCurrent(summary);
        }

    }

    @Override
    public com.fuzzy.Reproducer<InfluxDBGlobalState> getLastReproducer() {
        return null;
    }

    @Override
    public String getLastQueryString() {
        return null;
    }





}

