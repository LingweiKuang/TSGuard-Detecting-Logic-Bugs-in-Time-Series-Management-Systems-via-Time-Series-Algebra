package com.fuzzy.common.tsaf;

import com.fuzzy.common.constant.GlobalConstant;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class EquationsManager {
    private volatile static EquationsManager instance;
    // database_table_timeSeries -> Equations
    private final Map<String, Equations> equationsMap = new HashMap<>();
    // base_seq <=> base_seq
    private final static Equations baseAndBaseEquations = Equations.genBaseAndBaseEquations();

    public static EquationsManager getInstance() {
        if (instance == null) {
            synchronized (EquationsManager.class) {
                if (instance == null)
                    instance = new EquationsManager();
            }
        }
        return instance;
    }

    public void deleteEquationsFromDatabase(String databaseName) {
        this.equationsMap.entrySet().removeIf(entry -> !entry.getKey().startsWith(databaseName));
    }

    public Equations getEquationsFromTimeSeries(String databaseName, String tableName, String TimeSeriesName) {
        if (TimeSeriesName.endsWith(GlobalConstant.BASE_TIME_SERIES_NAME)) return baseAndBaseEquations;

        if (!this.equationsMap.containsKey(genHashKey(databaseName, tableName, TimeSeriesName))) {
            log.error("{} --Column does not exist--", TimeSeriesName);
            throw new AssertionError();
        }
        return this.equationsMap.get(genHashKey(databaseName, tableName, TimeSeriesName));
    }

    public Equations initEquationsFromTimeSeries(String databaseName, String tableName, String TimeSeriesName,
                                                 TSAFDataType TSAFDataType) {
        if (TimeSeriesName.endsWith(GlobalConstant.BASE_TIME_SERIES_NAME)) return baseAndBaseEquations;

        if (!this.equationsMap.containsKey(genHashKey(databaseName, tableName, TimeSeriesName))) {
            Equations equations = Equations.randomGenEquations(TSAFDataType);
            this.equationsMap.put(genHashKey(databaseName, tableName, TimeSeriesName), equations);
        }
        return this.equationsMap.get(genHashKey(databaseName, tableName, TimeSeriesName));
    }

    public static Equations generateReciprocalEquations(BigDecimal k, BigDecimal c) {
        return Equations.genReciprocalEquations(k, c);
    }

    public String genHashKey(String databaseName, String tableName, String timeSeriesName) {
        return String.format("%s_%s_%s", databaseName, tableName, timeSeriesName);
    }
}
