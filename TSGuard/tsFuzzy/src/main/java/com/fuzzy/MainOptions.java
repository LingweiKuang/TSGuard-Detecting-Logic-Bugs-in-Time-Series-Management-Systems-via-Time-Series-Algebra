package com.fuzzy;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import java.util.Objects;

@Parameters(separators = "=", commandDescription = "Options applicable to all DBMS")
public class MainOptions {
    public static final int NO_SET_PORT = -1;
    public static final int NO_REDUCE_LIMIT = -1;
    public static final MainOptions DEFAULT_OPTIONS = new MainOptions();

    @Parameter(names = {"--help", "-h"}, description = "Lists all supported options and commands", help = true)
    private boolean help; // NOPMD

    @Parameter(names = {
            "--num-threads"}, description = "How many threads should run concurrently to test separate databases")
    private int nrConcurrentThreads = 16; // NOPMD

    @Parameter(names = {
            "--drop-database"}, description = "Should the database be dropped after testing is completed?")
    private boolean isDropDatabase = false;

    @Parameter(names = {
            "--random-seed"}, description = "A seed value != -1 that can be set to make the query and database generation deterministic")
    private long randomSeed = -1; // NOPMD

    @Parameter(names = {"--num-tries"}, description = "Specifies after how many found errors to stop testing")
    private int totalNumberTries = 100; // NOPMD

    @Parameter(names = {"--max-num-inserts"}, description = "Specifies how many INSERT statements should be issued")
    private int maxNumberInserts = 30; // NOPMD

    @Parameter(names = {
            "--max-expression-depth"}, description = "Specifies the maximum depth of randomly-generated expressions")
    private int maxExpressionDepth = 3; // NOPMD

    @Parameter(names = {
            "--num-queries"}, description = "Specifies the number of queries to be issued to a database before creating a new database")
    private int nrQueries = 100000; // NOPMD

    @Parameter(names = {
            "--start-timestamp"}, description = "Start timestamp of time-series data, Unit: s")
    private long startTimestampOfTSData = 1641024000L;

    @Parameter(names = {
            "--sampling-frequency"}, description = "Sampling frequency of time series data, Unit: s")
    private long samplingFrequency = 5;

    @Parameter(names = {
            "--precision"}, description = "values: s ms us ns")
    private String precision = "ms";

    @Parameter(names = {
            "--num-statement-kind-retries"}, description = "Specifies the number of times a specific statement kind (e.g., INSERT) should be retried when the DBMS indicates that it failed")
    private int nrStatementRetryCount = 1000; // NOPMD

    @Parameter(names = "--log-each-select", description = "Logs every statement issued", arity = 1)
    private boolean logEachSelect = true; // NOPMD

    @Parameter(names = "--log-execution-time", description = "Logs the execution time of each statement (requires --log-each-select to be enabled)", arity = 1)
    private boolean logExecutionTime = true; // NOPMD

    @Parameter(names = "--print-failed", description = "Logs failed insert, create and other statements without results", arity = 1)
    private boolean loggerPrintFailed = true; // NOPMD

    @Parameter(names = "--qpg-enable", description = "Enable the experimental feature Query Plan Guidance (QPG)", arity = 1)
    private boolean enableQPG;

    @Parameter(names = "--qpg-log-query-plan", description = "Logs the query plans of each query (requires --qpg-enable)", arity = 1)
    private boolean logQueryPlan;

    @Parameter(names = "--log-syntax-error-query", description = "Logs the query syntax error query", arity = 1)
    private boolean logSyntaxErrorQuery;

    @Parameter(names = "--qpg-max-interval", description = "The maximum number of iterations to mutate tables if no new query plans (requires --qpg-enable)")
    private static int qpgMaxInterval = 1000;

    @Parameter(names = "--qpg-reward-weight", description = "The weight (0-1) of last reward when updating weighted average reward. A higher value denotes average reward is more affected by the last reward (requires --qpg-enable)")
    private static double qpgk = 0.25;

    @Parameter(names = "--qpg-selection-probability", description = "The probability (0-1) of the random selection of mutators. A higher value (>0.5) favors exploration over exploitation. (requires --qpg-enable)")
    private static double qpgProbability = 0.7;

    @Parameter(names = "--username", description = "The user name used to log into the DBMS")
    private String userName = "root"; // NOPMD

    @Parameter(names = "--password", description = "The password used to log into the DBMS")
    private String password = "Gkmysql1@3"; // NOPMD

    @Parameter(names = "--host", description = "The host used to log into the DBMS")
    private String host = "127.0.0.1";

    @Parameter(names = "--port", description = "The port used to log into the DBMS")
    private int port = MainOptions.NO_SET_PORT; // NOPMD

    @Parameter(names = "--params", description = "other params")
    private String params = "{}";

    @Parameter(names = "--print-progress-information", description = "Whether to print progress information such as the number of databases generated or queries issued", arity = 1)
    private boolean printProgressInformation = true; // NOPMD

    @Parameter(names = "--print-progress-summary", description = "Whether to print an execution summary when exiting TSGuard", arity = 1)
    private boolean printProgressSummary; // NOPMD

    @Parameter(names = "--timeout-seconds", description = "The timeout in seconds")
    private int timeoutSeconds = -1; // NOPMD

    @Parameter(names = "--max-generated-databases", description = "The maximum number of databases that are generated by each thread")
    private int maxGeneratedDatabases = -1; // NOPMD

    @Parameter(names = "--exit-code-error", description = "The exit code that should be returned when an error is encountered (or a bug is found)")
    private int errorExitCode = -1; // NOPMD

    @Parameter(names = "--print-statements", description = "Print all statements to stdout, before they are sent to the DBMS (not yet implemented for all oracles)", arity = 1)
    private boolean printStatements; // NOPMD

    @Parameter(names = "--print-succeeding-statements", description = "Print statements that are successfully processed by the DBMS to stdout (not yet implemented for all oracles)", arity = 1)
    private boolean printSucceedingStatements; // NOPMD

    @Parameter(names = "--test-only-nonempty-tables", description = "Test only databases each of whose tables contain at least a single row", arity = 1)
    private boolean testOnlyWithMoreThanZeroRows; // NOPMD

    @Parameter(names = "--pqs-test-aggregates", description = "Partially test aggregate functions when all tables contain only a single row.", arity = 1)
    private boolean testAggregateFunctions; // NOPMD

    @Parameter(names = "--random-string-generation", description = "Select the random-string eneration approach")
    private Randomly.StringGenerationStrategy randomStringGenerationStrategy = Randomly.StringGenerationStrategy.SOPHISTICATED; // NOPMD

    @Parameter(names = "--string-constant-max-length", description = "Specify the maximum-length of generated string constants")
    private int maxStringConstantLength = 10; // NOPMD

    @Parameter(names = "--use-constant-caching", description = "Specifies whether constants should be cached and re-used with a certain probability", arity = 1)
    private boolean useConstantCaching = true; // NOPMD

    @Parameter(names = "--use-connection-test", description = "Test whether the DBMS is accessible before trying to connect using multiple threads", arity = 1)
    private boolean useConnectionTest = true; // NOPMD

    @Parameter(names = "--constant-cache-size", description = "Specifies the size of the constant cache. This option only takes effect when constant caching is enabled")
    private int constantCacheSize = 100; // NOPMD

    @Parameter(names = "--database-prefix", description = "The prefix used for each database created")
    private String databasePrefix = "database"; // NOPMD

    @Parameter(names = "--use-reducer", description = "EXPERIMENTAL Attempt to reduce queries using a simple reducer")
    private boolean useReducer = false; // NOPMD

    @Parameter(names = "--statement-reducer-max-steps", description = "EXPERIMENTAL Maximum steps the statement reducer will do")
    private long maxStatementReduceSteps = NO_REDUCE_LIMIT; // NOPMD

    @Parameter(names = "--statement-reducer-max-time", description = "EXPERIMENTAL Maximum time duration (secs) the statement reducer will do")
    private long maxStatementReduceTime = NO_REDUCE_LIMIT;

    @Parameter(names = "--use-syntax-sequence", description = "")
    private boolean useSyntaxSequence = false;

    @Parameter(names = "--use-syntax-validator", description = "")
    private boolean useSyntaxValidator = false;

    public int getMaxExpressionDepth() {
        return maxExpressionDepth;
    }

    public int getTotalNumberTries() {
        return totalNumberTries;
    }

    public int getNumberConcurrentThreads() {
        return nrConcurrentThreads;
    }

    public boolean logEachSelect() {
        return logEachSelect;
    }

    public boolean printAllStatements() {
        if (printSucceedingStatements && printStatements) {
            throw new AssertionError();
        }
        return printStatements;
    }

    public boolean printSucceedingStatements() {
        if (printStatements && printSucceedingStatements) {
            throw new AssertionError();
        }
        return printSucceedingStatements;
    }

    public boolean logExecutionTime() {
        if (!logEachSelect) {
            throw new AssertionError();
        }
        return logExecutionTime;
    }

    public boolean isDropDatabase() {
        return isDropDatabase;
    }

    public boolean loggerPrintFailed() {
        return loggerPrintFailed;
    }

    public boolean logQueryPlan() {
        return logQueryPlan;
    }

    public boolean logSyntaxErrorQuery() {
        return logSyntaxErrorQuery;
    }

    public boolean enableQPG() {
        return enableQPG;
    }

    public int getQPGMaxMutationInterval() {
        return qpgMaxInterval;
    }

    public double getQPGk() {
        return qpgk;
    }

    public double getQPGProbability() {
        return qpgProbability;
    }

    public int getNrQueries() {
        return nrQueries;
    }

    public long getStartTimestampOfTSData() {
        switch (precision) {
            case "ms":
                return startTimestampOfTSData * 1000;
            case "us":
                return startTimestampOfTSData * 1000 * 1000;
            case "ns":
                return startTimestampOfTSData * 1000 * 1000 * 1000;
            case "s":
            default:
                return startTimestampOfTSData;
        }
    }

    public long getSamplingFrequency() {
        switch (precision) {
            case "ms":
                return samplingFrequency * 1000;
            case "us":
                return samplingFrequency * 1000 * 1000;
            case "ns":
                return samplingFrequency * 1000 * 1000 * 1000;
            case "s":
            default:
                return samplingFrequency;
        }
    }

    public String getPrecision() {
        return precision;
    }

    public String getParams() {
        return params;
    }

    public int getMaxNumberInserts() {
        return maxNumberInserts;
    }

    public int getNrStatementRetryCount() {
        return nrStatementRetryCount;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public boolean printProgressInformation() {
        return printProgressInformation;
    }

    public boolean printProgressSummary() {
        return printProgressSummary;
    }

    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public int getMaxGeneratedDatabases() {
        return maxGeneratedDatabases;
    }

    public int getErrorExitCode() {
        return errorExitCode;
    }

    public long getRandomSeed() {
        return randomSeed;
    }

    public boolean testAggregateFunctionsPQS() {
        return testAggregateFunctions;
    }

    public boolean testOnlyWithMoreThanZeroRows() {
        return testOnlyWithMoreThanZeroRows;
    }

    public Randomly.StringGenerationStrategy getRandomStringGenerationStrategy() {
        return randomStringGenerationStrategy;
    }

    public int getMaxStringConstantLength() {
        return maxStringConstantLength;
    }

    public boolean useConstantCaching() {
        return useConstantCaching;
    }

    public int getConstantCacheSize() {
        return constantCacheSize;
    }

    public boolean isHelp() {
        return help;
    }

    public boolean isDefaultPassword() {
        return Objects.equals(password, DEFAULT_OPTIONS.password);
    }

    public boolean isDefaultUsername() {
        return Objects.equals(userName, DEFAULT_OPTIONS.userName);
    }

    public String getDatabasePrefix() {
        return databasePrefix;
    }

    public boolean performConnectionTest() {
        return useConnectionTest;
    }

    public boolean useReducer() {
        return useReducer;
    }

    public long getMaxStatementReduceSteps() {
        return maxStatementReduceSteps;
    }

    public long getMaxStatementReduceTime() {
        return maxStatementReduceTime;
    }

    public boolean useSyntaxSequence() {
        return useSyntaxSequence;
    }

    public boolean useSyntaxValidator() {
        return useSyntaxValidator;
    }
}
