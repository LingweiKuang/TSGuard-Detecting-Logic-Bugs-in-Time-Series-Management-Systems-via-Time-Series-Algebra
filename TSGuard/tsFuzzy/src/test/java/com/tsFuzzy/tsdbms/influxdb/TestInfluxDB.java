    package com.tsFuzzy.tsdbms.influxdb;

    import com.fuzzy.Main;
    import com.fuzzy.common.constant.GlobalConstant;
    import com.tsFuzzy.tsdbms.TestConfig;
    import org.junit.Test;

    import static org.junit.Assert.assertEquals;

    public class TestInfluxDB {

        @Test
        public void testPQS() {
            assertEquals(0,
                    Main.executeMain(new String[]{"--random-seed", "-1", "--timeout-seconds", TestConfig.SECONDS,
                            "--num-threads", "1", "--host", "localhost", "--port", "8086", "--precision", "ms",
                            "--log-syntax-error-query", "true", "--num-tries", "1000", "--max-expression-depth", "4",
                            "--params", "{\"organizationId\":\"1db4a79f8d1dc77c\",\"token\":\"yQmxE44rWrGffiK_O9xgTiVz-__nXmkoS0zsrAZ3aEVup9jHu_tRaJO_aJpam9DQp99NAE8UoSgS9H66fFxCiQ==\"}",
                            "--random-string-generation", "ALPHANUMERIC", "--database-prefix",
                            "pqsdb", "--max-generated-databases", "1"/* Workaround for connections not being closed */,
                            "--num-queries", TestConfig.NUM_QUERIES, GlobalConstant.INFLUXDB_DATABASE_NAME, "--oracle", "PQS"}));
        }

        @Test
        public void testTSAF() {
            assertEquals(0,
                    Main.executeMain(new String[]{
                            "--random-seed", "-1",
                            "--timeout-seconds", TestConfig.SECONDS,
                            "--num-threads", "1",
                            "--host", "localhost",
                            "--port", "8086",
                            "--precision", "ns",
                            "--log-syntax-error-query", "true",
                            "--log-each-select","true",
                            "--max-expression-depth", "4",
                            "--log-execution-time", "false",
                            "--num-tries", "5", //increase in real test
                            "--params", "{\"organizationId\":\"1db4a79f8d1dc77c\",\"token\":\"yQmxE44rWrGffiK_O9xgTiVz-__nXmkoS0zsrAZ3aEVup9jHu_tRaJO_aJpam9DQp99NAE8UoSgS9H66fFxCiQ==\"}",
                            "--use-syntax-validator",
                            "--use-syntax-sequence",
                            "--random-string-generation", "ALPHANUMERIC",
                            "--database-prefix", "tsafdb",
                            "--max-generated-databases", "1"/* Workaround for connections not being closed */,
                            "--hint-frequency","1",
                            "--num-queries", TestConfig.NUM_QUERIES, GlobalConstant.INFLUXDB_DATABASE_NAME,
                            "--oracle", "TSAF"}));
        }

        @Test
        public void testHINT() {
            assertEquals(0,
                    Main.executeMain(new String[]{
                            "--random-seed", "-1",
                            "--timeout-seconds", TestConfig.SECONDS,
                            "--num-threads", "1",
                            "--host", "localhost",
                            "--port", "8086",
                            "--precision", "ms",
                            "--log-syntax-error-query", "true",
                            "--log-each-select", "true",
                            "--num-tries", "1000",   //increase more in real test
                            "--max-expression-depth", "4",
                            "--params", "{\"organizationId\":\"1db4a79f8d1dc77c\",\"token\":\"yQmxE44rWrGffiK_O9xgTiVz-__nXmkoS0zsrAZ3aEVup9jHu_tRaJO_aJpam9DQp99NAE8UoSgS9H66fFxCiQ==\"}",
                            "--use-syntax-validator",
                            "--use-syntax-sequence",
                            "--random-string-generation", "ALPHANUMERIC",
                            "--database-prefix", "hintdb",
                            "--max-generated-databases", "1",
                            "--num-queries", "1500",   //increase more in real test
                            GlobalConstant.INFLUXDB_DATABASE_NAME,
                            "--oracle", "HINT",

                    }));
        }

        @Test
        public void testTSAFWithoutC() {
            assertEquals(0,
                    Main.executeMain(new String[]{
                            "--random-seed", "-1",
                            "--timeout-seconds", TestConfig.SECONDS,
                            "--num-threads", "1",
                            "--host", "172.29.185.200",
                            "--port", "8086",
                            "--precision", "ns",
                            "--log-syntax-error-query", "true",
                            "--max-expression-depth", "4",
                            "--log-execution-time", "false",
                            "--num-tries", "2000",
                            "--use-syntax-sequence",
                            "--params", "{\"organizationId\":\"e8a8738626052670\",\"token\":\"pCGSL9nyhswIxaOjZnd59NGHxiFEh9yrtOViWqc4W8062eF1SHRbBoA-NlrFAgUOo5KuB6Bq3hRWNsuet6AdRQ==\"}",
                            "--random-string-generation", "ALPHANUMERIC",
                            "--database-prefix", "tsafdb",
                            "--max-generated-databases", "1"/* Workaround for connections not being closed */,
                            "--num-queries", TestConfig.NUM_QUERIES, GlobalConstant.INFLUXDB_DATABASE_NAME,
                            "--oracle", "TSAF"
                    }));
        }

        @Test
        public void testTSAFWithoutS() {
            assertEquals(0,
                    Main.executeMain(new String[]{"--random-seed", "-1", "--timeout-seconds", TestConfig.SECONDS,
                            "--num-threads", "1", "--host", "172.29.185.200", "--port", "8086", "--precision", "ns",
                            "--log-syntax-error-query", "true", "--max-expression-depth", "4",
                            "--log-execution-time", "false", "--num-tries", "2000",
                            "--use-syntax-validator",
                            "--params", "{\"organizationId\":\"e8a8738626052670\",\"token\":\"pCGSL9nyhswIxaOjZnd59NGHxiFEh9yrtOViWqc4W8062eF1SHRbBoA-NlrFAgUOo5KuB6Bq3hRWNsuet6AdRQ==\"}",
                            "--random-string-generation", "ALPHANUMERIC", "--database-prefix",
                            "tsafdb", "--max-generated-databases", "1"/* Workaround for connections not being closed */,
                            "--num-queries", TestConfig.NUM_QUERIES, GlobalConstant.INFLUXDB_DATABASE_NAME, "--oracle", "TSAF"}));
        }

        @Test
        public void testTSAFWithoutAll() {
            assertEquals(0,
                    Main.executeMain(new String[]{"--random-seed", "-1", "--timeout-seconds", TestConfig.SECONDS,
                            "--num-threads", "1", "--host", "172.29.185.200", "--port", "8086", "--precision", "ns",
                            "--log-syntax-error-query", "true", "--max-expression-depth", "4",
                            "--log-execution-time", "false", "--num-tries", "2000",
                            "--params", "{\"organizationId\":\"e8a8738626052670\",\"token\":\"pCGSL9nyhswIxaOjZnd59NGHxiFEh9yrtOViWqc4W8062eF1SHRbBoA-NlrFAgUOo5KuB6Bq3hRWNsuet6AdRQ==\"}",
                            "--random-string-generation", "ALPHANUMERIC", "--database-prefix",
                            "tsafdb", "--max-generated-databases", "1"/* Workaround for connections not being closed */,
                            "--num-queries", TestConfig.NUM_QUERIES, GlobalConstant.INFLUXDB_DATABASE_NAME, "--oracle", "TSAF"}));
        }



    }
