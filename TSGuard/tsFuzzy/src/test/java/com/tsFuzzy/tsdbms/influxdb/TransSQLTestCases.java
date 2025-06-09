package com.tsFuzzy.tsdbms.influxdb;

import com.fuzzy.common.coverage.TransSQLTestCaseUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class TransSQLTestCases {
    private static final String sourceSQLFilePath = "C:\\user\\timmy\\Desktop\\TSGuard\\TSGuard-Detecting-Logic-Bugs-in-Time-Series-Management-Systems-via-Time-Series-Algebra\\TSGuard\\tsFuzzy\\logs\\influxdb";
    private static final String targetSQLFilePath = "C:\\user\\timmy\\Desktop\\TSGuard\\TSGuard-Detecting-Logic-Bugs-in-Time-Series-Management-Systems-via-Time-Series-Algebra\\TSGuard\\tsFuzzy\\logs\\influxdb\\query.rs";
    //C:\Users\timmy\Desktop\TSGuard\TSGuard-Detecting-Logic-Bugs-in-Time-Series-Management-Systems-via-Time-Series-Algebra\TSGuard\tsFuzzy
    @Test
    public void getSamplingFrequency() throws Exception {
        TransSQLTestCaseUtils.transSQLTestCaseInDirForInfluxDB(sourceSQLFilePath, targetSQLFilePath);
    }
}
