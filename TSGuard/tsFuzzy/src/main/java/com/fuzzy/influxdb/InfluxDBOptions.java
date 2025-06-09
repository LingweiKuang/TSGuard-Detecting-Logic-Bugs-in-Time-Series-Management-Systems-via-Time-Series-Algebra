package com.fuzzy.influxdb;

import com.benchmark.entity.DBValResultSet;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fuzzy.DBMSSpecificOptions;
import com.fuzzy.OracleFactory;
import com.fuzzy.SQLConnection;
import com.fuzzy.common.oracle.TestOracle;
import com.fuzzy.common.query.Query;
import com.fuzzy.common.tsaf.TimeSeriesConstraint;
import com.fuzzy.influxdb.InfluxDBOptions.InfluxDBOracleFactory;
import com.fuzzy.influxdb.oracle.InfluxDBHintOracle;
import com.fuzzy.influxdb.oracle.InfluxDBPivotedQuerySynthesisOracle;
import com.fuzzy.influxdb.oracle.InfluxDBTSAFOracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Parameters(separators = "=", commandDescription = "InfluxDB (default port: " + InfluxDBOptions.DEFAULT_PORT
        + ", default host: " + InfluxDBOptions.DEFAULT_HOST + ")")
public class InfluxDBOptions implements DBMSSpecificOptions<InfluxDBOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 8086;

    @Parameter(names = "--hint-frequency", description = "How many times to run HINT oracle per bucket")
    public int hintFrequency = 1;

    @Parameter(names = "--oracle")
    public List<InfluxDBOracleFactory> oracles = Arrays.asList(InfluxDBOracleFactory.HINT);

    @Override
    public List<InfluxDBOracleFactory> getTestOracleFactory() {
        List<InfluxDBOracleFactory> result = new ArrayList<>();
        for (int i = 0; i < hintFrequency; i++){
            result.addAll(oracles);
        }
        return result;
    }

    public enum InfluxDBOracleFactory implements OracleFactory<InfluxDBGlobalState> {
        PQS {
            @Override
            public TestOracle<InfluxDBGlobalState> create(InfluxDBGlobalState globalState) throws SQLException {
                return new InfluxDBPivotedQuerySynthesisOracle(globalState);
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }

        },
        TSAF {
            @Override
            public TestOracle<InfluxDBGlobalState> create(InfluxDBGlobalState globalState) throws SQLException {
                return new InfluxDBTSAFOracle(globalState) {
                    @Override
                    protected boolean containsRows(TimeSeriesConstraint constraint) {
                        return false;
                    }
                };
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }
        },
        HINT {
            @Override
            public TestOracle<InfluxDBGlobalState> create(InfluxDBGlobalState globalState) {
                return new InfluxDBHintOracle(globalState);
            }

            @Override public boolean requiresAllTablesToContainRows() {
                return true;
            }
        };
    }
}
