package com.fuzzy.influxdb;

import com.fuzzy.SQLGlobalState;
import com.fuzzy.influxdb.InfluxDBOptions.InfluxDBOracleFactory;

public class InfluxDBGlobalState extends SQLGlobalState<InfluxDBOptions, InfluxDBSchema> {

    @Override
    protected InfluxDBSchema readSchema() throws Exception {
        return InfluxDBSchema.fromConnection(getConnection(), getDatabaseName());
    }

    public boolean usesPQS() {
        return getDbmsSpecificOptions().oracles.stream().anyMatch(o -> o == InfluxDBOracleFactory.PQS);
    }

    public boolean usesTSAF() {
        return getDbmsSpecificOptions().oracles.stream().anyMatch(o -> o == InfluxDBOptions.InfluxDBOracleFactory.TSAF);
    }

    public boolean usesHINT(){
        return getDbmsSpecificOptions().oracles.stream().anyMatch((o -> o == InfluxDBOptions.InfluxDBOracleFactory.HINT));
    }
}
