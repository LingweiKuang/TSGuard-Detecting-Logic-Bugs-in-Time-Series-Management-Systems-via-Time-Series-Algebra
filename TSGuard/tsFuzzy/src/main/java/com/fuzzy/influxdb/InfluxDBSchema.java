package com.fuzzy.influxdb;

import com.benchmark.commonClass.TSFuzzyStatement;
import com.fuzzy.GlobalState;
import com.fuzzy.Randomly;
import com.fuzzy.SQLConnection;
import com.fuzzy.common.schema.*;
import com.fuzzy.common.tsaf.TSAFDataType;
import com.fuzzy.influxdb.ast.InfluxDBConstant;
import com.fuzzy.influxdb.resultSet.InfluxDBResultSet;
import com.fuzzy.influxdb.resultSet.InfluxDBSeries;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class InfluxDBSchema extends AbstractSchema<InfluxDBGlobalState, InfluxDBSchema.InfluxDBTable> {

    private static final int NR_SCHEMA_READ_TRIES = 10;

    public InfluxDBSchema(List<InfluxDBTable> databaseTables) {
        super(databaseTables);
    }

    @Override
    public boolean containsTableWithZeroRows(InfluxDBGlobalState globalState) {
        // InfluxDB 默认插入一行数据, 故检测空值改为1
        return getDatabaseTables().stream().anyMatch(t -> t.getNrRows(globalState) == 1);
    }

    public static InfluxDBSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        Exception ex = null;
        /* the loop is a workaround for https://bugs.mysql.com/bug.php?id=95929 */
        for (int i = 0; i < NR_SCHEMA_READ_TRIES; i++) {
            try {
                // databaseName = "bucket";
                List<InfluxDBTable> databaseTables = new ArrayList<>();
                try (TSFuzzyStatement s = con.createStatement()) {
                    try (InfluxDBResultSet rs = (InfluxDBResultSet)
                            s.executeQuery("q=SHOW MEASUREMENTS ON " + databaseName)) {
                        while (rs.hasNext()) {
                            // TODO
                            InfluxDBSeries influxDBSeries = rs.getCurrentValue();
                            if (influxDBSeries.getValues().isEmpty()) break;
                            List<String> measurements = influxDBSeries.getAllValues();
                            for (String measurement : measurements) {
                                List<InfluxDBColumn> databaseColumns = getTableTagColumns(con, measurement, databaseName);
                                databaseColumns.addAll(getTableFieldColumns(con, measurement, databaseName));
                                // List<InfluxDBIndex> indexes = getIndexes(con, tableName, databaseName);
                                InfluxDBTable t = new InfluxDBTable(measurement, databaseName, databaseColumns, null,
                                        getSeriesForMeasurement(con, measurement, databaseName));
                                for (InfluxDBColumn c : databaseColumns) {
                                    c.setTable(t);
                                }
                                databaseTables.add(t);
                            }
                        }
                    }
                }
                return new InfluxDBSchema(databaseTables);
            } catch (SQLIntegrityConstraintViolationException e) {
                ex = e;
            }
        }
        throw new AssertionError(ex);
    }

    private static List<InfluxDBColumn> getTableTagColumns(SQLConnection con, String tableName, String databaseName)
            throws SQLException {
        List<InfluxDBColumn> influxDBColumns = new ArrayList<>();
        String sql = "q=SHOW TAG KEYS ON " + databaseName;
        try (TSFuzzyStatement s = con.createStatement()) {
            try (InfluxDBResultSet rs = (InfluxDBResultSet) s.executeQuery(sql)) {
                while (rs.hasNext()) {

                    InfluxDBSeries dbSeries = rs.getCurrentValue();
                    if (!StringUtils.equalsIgnoreCase(tableName, dbSeries.getName())) {
                        continue;
                    }

                    dbSeries.getValues().forEach(columns -> {
                        // tag列数据仅含一个值
                        InfluxDBColumn influxDBColumn = new InfluxDBColumn(columns.get(0), true,
                                InfluxDBDataType.STRING);
                        influxDBColumns.add(influxDBColumn);
                    });
                }
            }
        }
        return influxDBColumns;
    }

    private static List<InfluxDBColumn> getTableFieldColumns(SQLConnection con, String tableName, String databaseName)
            throws SQLException {
        List<InfluxDBColumn> influxDBColumns = new ArrayList<>();
        String sql = "q=SHOW FIELD KEYS ON " + databaseName;
        try (TSFuzzyStatement s = con.createStatement()) {
            try (InfluxDBResultSet rs = (InfluxDBResultSet) s.executeQuery(sql)) {
                while (rs.hasNext()) {

                    InfluxDBSeries dbSeries = rs.getCurrentValue();
                    if (!StringUtils.equalsIgnoreCase(tableName, dbSeries.getName())) {
                        continue;
                    }

                    int fieldKeyIndex = 0, fieldTypeIndex = 1;
                    for (int i = 0; i < dbSeries.getColumns().size(); i++) {
                        if ("fieldKey".equalsIgnoreCase(dbSeries.getColumns().get(i))) fieldKeyIndex = i;
                        else if ("fieldType".equalsIgnoreCase(dbSeries.getColumns().get(i))) fieldTypeIndex = i;
                    }
                    for (List<String> columns : dbSeries.getValues()) {
                        InfluxDBColumn influxDBColumn = new InfluxDBColumn(columns.get(fieldKeyIndex), false,
                                InfluxDBDataType.getByString(columns.get(fieldTypeIndex)));
                        influxDBColumns.add(influxDBColumn);
                    }
                }
            }
        }
        return influxDBColumns;
    }

    private static Set<String> getSeriesForMeasurement(SQLConnection con, String measurement, String databaseName)
            throws SQLException {
        Set<String> seriesOfMeasurement = new HashSet<>();
        String sql = "q=SHOW SERIES ON " + databaseName;
        try (TSFuzzyStatement s = con.createStatement()) {
            try (InfluxDBResultSet rs = (InfluxDBResultSet) s.executeQuery(sql)) {
                while (rs.hasNext()) {

                    InfluxDBSeries dbSeries = rs.getCurrentValue();

                    for (List<String> seriesList : dbSeries.getValues()) {
                        String series = seriesList.get(0);
                        if (series.startsWith(measurement)) seriesOfMeasurement.add(series);
                    }
                }
            }
        }
        return seriesOfMeasurement;
    }

    public enum InfluxDBDataType {
        INT("integer"), UINT("unsigned"), FLOAT("float"),
        STRING("string"), BOOLEAN("boolean"), BIGDECIMAL("bigDecimal"),
        TIMESTAMP("TIMESTAMP");

        private String typeName;

        InfluxDBDataType(String typeName) {
            this.typeName = typeName;
        }

        public String getTypeName() {
            return typeName;
        }

        public static InfluxDBDataType[] valuesPQS() {
            return new InfluxDBDataType[]{FLOAT};
        }

        public static InfluxDBDataType[] valuesTSAF() {
            return new InfluxDBDataType[]{FLOAT};
        }

        public static InfluxDBDataType getRandom(InfluxDBGlobalState globalState) {
            if (globalState.usesPQS()) {
                return Randomly.fromOptions(valuesPQS());
            } else if (globalState.usesTSAF()) {
                return Randomly.fromOptions(valuesTSAF());
            } else {
                return Randomly.fromOptions(values());
            }
        }

        public TSAFDataType toTSAFDataType() {
            switch (this) {
                case INT:
                    return TSAFDataType.INT;
                case UINT:
                    return TSAFDataType.UINT;
                case FLOAT:
                    return TSAFDataType.DOUBLE;
                default:
                    throw new AssertionError();
            }
        }

        public static InfluxDBDataType getRandom() {
            return Randomly.fromOptions(values());
        }

        public Object getInitValueByType(GlobalState globalState) {
            switch (this) {
                case INT:
                    return 0 + "i";
                case UINT:
                    return 0 + "u";
                case FLOAT:
                case BIGDECIMAL:
                    return BigDecimal.ZERO.doubleValue();
                case STRING:
                    // return globalState.getRandomly().getString().replace("\\", "").replace("\n", "");
                    return "\"0\"";
                case BOOLEAN:
                    return Randomly.getBoolean();
                case TIMESTAMP:
                    return System.currentTimeMillis();

                default:
                    throw new AssertionError(this);
            }
        }

        public static InfluxDBDataType getByString(String s) {
            if (InfluxDBDataType.INT.getTypeName().equalsIgnoreCase(s))
                return InfluxDBDataType.INT;
            else if (InfluxDBDataType.UINT.getTypeName().equalsIgnoreCase(s))
                return InfluxDBDataType.UINT;
            else if (InfluxDBDataType.FLOAT.getTypeName().equalsIgnoreCase(s))
                return InfluxDBDataType.FLOAT;
            else if (InfluxDBDataType.STRING.getTypeName().equalsIgnoreCase(s))
                return InfluxDBDataType.STRING;
            else if (InfluxDBDataType.BOOLEAN.getTypeName().equalsIgnoreCase(s))
                return InfluxDBDataType.BOOLEAN;
            else {
                throw new AssertionError(String.format("InfluxDB中数据类型无法解析:%s", s));
            }
        }

        public boolean isNumeric() {
            switch (this) {
                case INT:
                case UINT:
                case FLOAT:
                    return true;
                case STRING:
                case BOOLEAN:
                    return false;
                default:
                    throw new AssertionError(this);
            }
        }

        public boolean isInt() {
            switch (this) {
                case INT:
                case UINT:
                    return true;
                case FLOAT:
                case STRING:
                case BOOLEAN:
                    return false;
                default:
                    throw new AssertionError(this);
            }
        }
    }

    public static class InfluxDBColumn extends AbstractTableColumn<InfluxDBTable, InfluxDBDataType> {


        public InfluxDBColumn(String name, boolean isTag, InfluxDBDataType type) {
            super(name, null, type, isTag);
        }

    }

    public static class InfluxDBTable extends AbstractRelationalTable<InfluxDBColumn, InfluxDBIndex, InfluxDBGlobalState> {

        // measurement,tags=values
        private final Set<String> seriesSet;

        public InfluxDBTable(String name, String databaseName, List<InfluxDBColumn> columns, List<InfluxDBIndex> indexes, Set<String> seriesSet) {
            super(name, databaseName, columns, indexes, false);
            this.seriesSet = seriesSet;
        }

        @Override
        public String getSelectCountTableName() {
            return String.format("%s.autogen.%s", databaseName, name);
        }

        @Override
        public String selectCountStatement() {
            return "q=SELECT COUNT(*) FROM " + this.getSelectCountTableName();
        }

        public String getFullName() {
            return String.format("%s.autogen.%s", databaseName, name);
        }

        public Set<String> getSeriesSet() {
            return seriesSet;
        }

        public String getRandomSeries() {
            return Randomly.fromList(new ArrayList<>(seriesSet));
        }

        public InfluxDBSeries getRowValues(SQLConnection con, String precision) throws SQLException {
            String sql = String.format("q=SELECT * FROM %s ORDER BY time", this.getFullName());
            // 将time字段按照指定时间戳形式返回
            sql += String.format("&epoch=%s", precision);

            try (TSFuzzyStatement s = con.createStatement()) {
                InfluxDBResultSet dbResultSet = (InfluxDBResultSet) s.executeQuery(sql);
                if (!dbResultSet.hasNext()) {
                    throw new AssertionError("could not find random row! " + sql + "\n");
                }

                // 查询仅返回指定的一条时序
                assert dbResultSet.getSeriesList().size() == 1;
                return dbResultSet.getCurrentValue();
            }
        }
    }

    public static final class InfluxDBIndex extends TableIndex {

        protected InfluxDBIndex(String indexName) {
            super(indexName);
        }

    }

    public static class InfluxDBRowValue extends AbstractRowValue<InfluxDBTables, InfluxDBColumn, InfluxDBConstant> {

        InfluxDBRowValue(InfluxDBTables tables, Map<InfluxDBColumn, InfluxDBConstant> values) {
            super(tables, values);
        }

    }

    public static class InfluxDBTables extends AbstractTables<InfluxDBTable, InfluxDBColumn> {

        public InfluxDBTables(List<InfluxDBTable> tables) {
            super(tables);
        }

        @Override
        public String tableNamesAsString() {
            return getTables().stream().map(InfluxDBTable::getFullName).collect(Collectors.joining(","));
        }

        public InfluxDBRowValue getRandomRowValue(GlobalState globalState) throws SQLException {
            // TODO 会针对各个时序数据分别查询具体字段值, 不存在则会置为nil
            SQLConnection con = (SQLConnection) globalState.getConnection();
            long rowCount = this.getTables().get(0).getNrRows((InfluxDBGlobalState) globalState);
            String randomRow = String.format("q=SELECT %s FROM %s ORDER BY time LIMIT 1 OFFSET %d",
                    columnNamesAsString(AbstractTableColumn::getName),
                    tableNamesAsString(), globalState.getRandomly().getLong(0, rowCount));
            Map<InfluxDBColumn, InfluxDBConstant> values = new HashMap<>();
            try (TSFuzzyStatement s = con.createStatement()) {
                InfluxDBResultSet randomRowValues = (InfluxDBResultSet) s.executeQuery(randomRow);
                if (!randomRowValues.hasNext()) {
                    throw new AssertionError("could not find random row! " + randomRow + "\n");
                }
                for (int i = 0; i < getColumns().size(); i++) {
                    InfluxDBColumn column = getColumns().get(i);
                    Object value;
                    int columnIndex = randomRowValues.findColumn(column.getName());
                    // assert columnIndex == i + 1;
                    InfluxDBConstant constant;
                    if (randomRowValues.getString(columnIndex) == null) {
                        constant = InfluxDBConstant.createNullConstant();
                    } else {
                        switch (column.getType()) {
                            case INT:
                                value = randomRowValues.getLong(columnIndex);
                                constant = InfluxDBConstant.createIntConstant((long) value);
                                break;
                            case BOOLEAN:
                                value = randomRowValues.getString(columnIndex);
                                constant = InfluxDBConstant.createBoolean(
                                        ((String) value).equalsIgnoreCase("true"));
                                break;
                            case UINT:
                                value = randomRowValues.getLong(columnIndex);
                                constant = InfluxDBConstant.createIntConstant((long) value, false);
                                break;
                            case STRING:
                                value = randomRowValues.getString(columnIndex);
                                constant = InfluxDBConstant.createSingleQuotesStringConstant((String) value);
                                break;
                            case FLOAT:
                            case BIGDECIMAL:
                                value = randomRowValues.getDouble(columnIndex);
                                constant = InfluxDBConstant.createDoubleConstant((double) value);
                                break;
                            default:
                                throw new AssertionError(column.getType());
                        }
                    }
                    values.put(column, constant);
                }
                return new InfluxDBRowValue(this, values);
            }
        }

    }

    public InfluxDBTables getRandomTableNonEmptyTables() {
        // 时序数据库不测试cross join, 返回一张表即可
        return new InfluxDBTables(Randomly.nonEmptySubset(getDatabaseTables(), 1));
    }
}
