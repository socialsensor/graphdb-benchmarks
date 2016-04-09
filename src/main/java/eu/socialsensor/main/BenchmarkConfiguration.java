package eu.socialsensor.main;

import java.io.File;
import java.util.*;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.math3.util.CombinatoricsUtils;

import com.amazon.titan.diskstorage.dynamodb.BackendDataModel;
import com.amazon.titan.diskstorage.dynamodb.Constants;
import com.google.common.primitives.Ints;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

import eu.socialsensor.dataset.DatasetFactory;
import jp.classmethod.titan.diskstorage.tupl.TuplStoreManager;

/**
 * 
 * @author Alexander Patrikalakis
 *
 */
public class BenchmarkConfiguration
{
    // Titan specific configuration
    private static final String TITAN = "titan";
    private static final String BUFFER_SIZE = GraphDatabaseConfiguration.BUFFER_SIZE.getName();
    private static final String IDS_BLOCKSIZE = GraphDatabaseConfiguration.IDS_BLOCK_SIZE.getName();
    private static final String PAGE_SIZE = GraphDatabaseConfiguration.PAGE_SIZE.getName();
    public static final String CSV_INTERVAL = GraphDatabaseConfiguration.METRICS_CSV_INTERVAL.getName();
    public static final String CSV = GraphDatabaseConfiguration.METRICS_CSV_NS.getName();
    private static final String CSV_DIR = GraphDatabaseConfiguration.METRICS_CSV_DIR.getName();
    public static final String GRAPHITE = GraphDatabaseConfiguration.METRICS_GRAPHITE_NS.getName();
    private static final String GRAPHITE_HOSTNAME = GraphDatabaseConfiguration.GRAPHITE_HOST.getName();
    private static final String CUSTOM_IDS = "custom-ids";

    // DynamoDB Storage Backend for Titan specific configuration
    private static final String CONSTRUCTOR_ARGS = Constants.DYNAMODB_CREDENTIALS_CONSTRUCTOR_ARGS.getName();
    private static final String CLASS_NAME = Constants.DYNAMODB_CREDENTIALS_CLASS_NAME.getName();
    private static final String CONSISTENT_READ = Constants.DYNAMODB_FORCE_CONSISTENT_READ.getName();
    private static final String TPS = "tps";
    private static final String CREDENTIALS = Constants.DYNAMODB_CLIENT_CREDENTIALS_NAMESPACE.getName();
    private static final String ENDPOINT = Constants.DYNAMODB_CLIENT_ENDPOINT.getName();
    private static final String TABLE_PREFIX = Constants.DYNAMODB_TABLE_PREFIX.getName();

    // benchmark configuration
    private static final String DATASET = "dataset";
    private static final String DATABASE_STORAGE_DIRECTORY = "database-storage-directory";
    private static final String ACTUAL_COMMUNITIES = "actual-communities";
    private static final String RANDOMIZE_CLUSTERING = "randomize-clustering";
    private static final String CACHE_PERCENTAGES = "cache-percentages";
    private static final String PERMUTE_BENCHMARKS = "permute-benchmarks";
    private static final String RANDOM_NODES = "shortest-path-random-nodes";
    private static final String RANDOM_SEED = "random-seed";
    private static final String MAX_HOPS = "shortest-path-max-hops";
    
    private static final Set<String> metricsReporters = new HashSet<String>();
    static {
        metricsReporters.add(CSV);
        metricsReporters.add(GRAPHITE);
    }

    private final File dataset;
    private final List<BenchmarkType> benchmarkTypes;
    private final SortedSet<GraphDatabaseType> selectedDatabases;
    private final File resultsPath;

    // storage directory
    private final File dbStorageDirectory;

    // metrics (optional)
    private final long csvReportingInterval;
    private final File csvDir;
    private final String graphiteHostname;
    private final long graphiteReportingInterval;

    // storage backend specific settings
    private final long dynamodbTps;
    private final BackendDataModel dynamodbDataModel;
    private final boolean dynamodbConsistentRead;

    // shortest path
    private final int numShortestPathRandomNodes;

    // clustering
    private final Boolean randomizedClustering;
    private final Integer cacheValuesCount;
    private final Double cacheIncrementFactor;
    private final List<Integer> cachePercentages;
    private final File actualCommunities;
    private final boolean permuteBenchmarks;
    private final int scenarios;
    private final String dynamodbCredentialsFqClassName;
    private final String dynamodbCredentialsCtorArguments;
    private final String dynamodbEndpoint;
    private final int bufferSize;
    private final int blocksize;
    private final int pageSize;
    private final int dynamodbWorkerThreads;
    private final boolean dynamodbPrecreateTables;
    private final String dynamodbTablePrefix;
    private final boolean customIds;
    private final long tuplMinCacheSize;
    private final int shortestPathMaxHops;

    private final Random random;

    public String getDynamodbCredentialsFqClassName()
    {
        return dynamodbCredentialsFqClassName;
    }

    public String getDynamodbCredentialsCtorArguments()
    {
        return dynamodbCredentialsCtorArguments;
    }

    public String getDynamodbEndpoint()
    {
        return dynamodbEndpoint;
    }

    public BenchmarkConfiguration(Configuration appconfig)
    {
        if (appconfig == null)
        {
            throw new IllegalArgumentException("appconfig may not be null");
        }

        Configuration eu = appconfig.subset("eu");
        Configuration socialsensor = eu.subset("socialsensor");
        
        //metrics
        final Configuration metrics = socialsensor.subset(GraphDatabaseConfiguration.METRICS_NS.getName());

        final Configuration graphite = metrics.subset(GRAPHITE);
        this.graphiteHostname = graphite.getString(GRAPHITE_HOSTNAME, null);
        this.graphiteReportingInterval = graphite.getLong(GraphDatabaseConfiguration.GRAPHITE_INTERVAL.getName(), 1000 /*default 1sec*/);

        final Configuration csv = metrics.subset(CSV);
        this.csvReportingInterval = metrics.getLong(CSV_INTERVAL, 1000 /*ms*/);
        this.csvDir = csv.containsKey(CSV_DIR) ? new File(csv.getString(CSV_DIR, System.getProperty("user.dir") /*default*/)) : null;

        Configuration dynamodb = socialsensor.subset("dynamodb");
        this.dynamodbWorkerThreads = dynamodb.getInt("workers", 25);
        Configuration credentials = dynamodb.subset(CREDENTIALS);
        this.dynamodbPrecreateTables = dynamodb.getBoolean("precreate-tables", Boolean.FALSE);
        this.dynamodbTps = Math.max(1, dynamodb.getLong(TPS, 750 /*default*/));
        this.dynamodbConsistentRead = dynamodb.containsKey(CONSISTENT_READ) ? dynamodb.getBoolean(CONSISTENT_READ)
            : false;
        this.dynamodbDataModel = dynamodb.containsKey("data-model") ? BackendDataModel.valueOf(dynamodb
            .getString("data-model")) : null;
        this.dynamodbCredentialsFqClassName = credentials.containsKey(CLASS_NAME) ? credentials.getString(CLASS_NAME)
            : null;
        this.dynamodbCredentialsCtorArguments = credentials.containsKey(CONSTRUCTOR_ARGS) ? credentials
            .getString(CONSTRUCTOR_ARGS) : null;
        this.dynamodbEndpoint = dynamodb.containsKey(ENDPOINT) ? dynamodb.getString(ENDPOINT) : null;
        this.dynamodbTablePrefix = dynamodb.containsKey(TABLE_PREFIX) ? dynamodb.getString(TABLE_PREFIX) : Constants.DYNAMODB_TABLE_PREFIX.getDefaultValue();

        Configuration titan = socialsensor.subset(TITAN); //TODO(amcp) move dynamodb ns into titan
        bufferSize = titan.getInt(BUFFER_SIZE, GraphDatabaseConfiguration.BUFFER_SIZE.getDefaultValue());
        blocksize = titan.getInt(IDS_BLOCKSIZE, GraphDatabaseConfiguration.IDS_BLOCK_SIZE.getDefaultValue());
        pageSize = titan.getInt(PAGE_SIZE, GraphDatabaseConfiguration.PAGE_SIZE.getDefaultValue());
        customIds = titan.getBoolean(CUSTOM_IDS, false /*default*/);

        final Configuration tupl = socialsensor.subset("tupl");
        tuplMinCacheSize = tupl.getLong(TuplStoreManager.TUPL_MIN_CACHE_SIZE.getName(), TuplStoreManager.TUPL_MIN_CACHE_SIZE.getDefaultValue());

        // database storage directory
        if (!socialsensor.containsKey(DATABASE_STORAGE_DIRECTORY))
        {
            throw new IllegalArgumentException("configuration must specify database-storage-directory");
        }
        dbStorageDirectory = new File(socialsensor.getString(DATABASE_STORAGE_DIRECTORY));
        dataset = validateReadableFile(socialsensor.getString(DATASET), DATASET);


        // load the dataset
        random = new Random(socialsensor.getInt(RANDOM_SEED, 17 /*default*/));
        numShortestPathRandomNodes = socialsensor.getInteger(RANDOM_NODES, new Integer(101));
        shortestPathMaxHops = socialsensor.getInteger(MAX_HOPS, 5);
        DatasetFactory.getInstance().createAndGetDataset(dataset, random, numShortestPathRandomNodes);

        if (!socialsensor.containsKey(PERMUTE_BENCHMARKS))
        {
            throw new IllegalArgumentException("configuration must set permute-benchmarks to true or false");
        }
        permuteBenchmarks = socialsensor.getBoolean(PERMUTE_BENCHMARKS);

        List<?> benchmarkList = socialsensor.getList("benchmarks");
        benchmarkTypes = new ArrayList<BenchmarkType>();
        for (Object str : benchmarkList)
        {
            benchmarkTypes.add(BenchmarkType.valueOf(str.toString()));
        }

        selectedDatabases = new TreeSet<GraphDatabaseType>();
        for (Object database : socialsensor.getList("databases"))
        {
            if (!GraphDatabaseType.STRING_REP_MAP.keySet().contains(database.toString()))
            {
                throw new IllegalArgumentException(String.format("selected database %s not supported",
                    database.toString()));
            }
            selectedDatabases.add(GraphDatabaseType.STRING_REP_MAP.get(database));
        }
        scenarios = permuteBenchmarks ? Ints.checkedCast(CombinatoricsUtils.factorial(selectedDatabases.size())) : 1;

        resultsPath = new File(System.getProperty("user.dir"), socialsensor.getString("results-path"));
        if (!resultsPath.exists() && !resultsPath.mkdirs())
        {
            throw new IllegalArgumentException("unable to create results directory");
        }
        if (!resultsPath.canWrite())
        {
            throw new IllegalArgumentException("unable to write to results directory");
        }

        if (this.benchmarkTypes.contains(BenchmarkType.CLUSTERING))
        {
            if (!socialsensor.containsKey(RANDOMIZE_CLUSTERING))
            {
                throw new IllegalArgumentException("the CW benchmark requires randomize-clustering bool in config");
            }
            randomizedClustering = socialsensor.getBoolean(RANDOMIZE_CLUSTERING);

            if (!socialsensor.containsKey(ACTUAL_COMMUNITIES))
            {
                throw new IllegalArgumentException("the CW benchmark requires a file with actual communities");
            }
            actualCommunities = validateReadableFile(socialsensor.getString(ACTUAL_COMMUNITIES), ACTUAL_COMMUNITIES);

            final boolean notGenerating = socialsensor.containsKey(CACHE_PERCENTAGES);
            if (notGenerating)
            {
                List<?> objects = socialsensor.getList(CACHE_PERCENTAGES);
                cachePercentages = new ArrayList<Integer>(objects.size());
                cacheValuesCount = null;
                cacheIncrementFactor = null;
                for (Object o : objects)
                {
                    cachePercentages.add(Integer.valueOf(o.toString()));
                }
            }
            else
            {
                throw new IllegalArgumentException(
                    "when doing CW benchmark, must provide cache-percentages");
            }
        }
        else
        {
            randomizedClustering = null;
            cacheValuesCount = null;
            cacheIncrementFactor = null;
            cachePercentages = null;
            actualCommunities = null;
        }
    }

    public File getDataset()
    {
        return dataset;
    }

    public SortedSet<GraphDatabaseType> getSelectedDatabases()
    {
        return selectedDatabases;
    }

    public File getDbStorageDirectory()
    {
        return dbStorageDirectory;
    }

    public File getResultsPath()
    {
        return resultsPath;
    }

    public long getDynamodbTps()
    {
        return dynamodbTps;
    }

    public boolean dynamodbConsistentRead()
    {
        return dynamodbConsistentRead;
    }

    public BackendDataModel getDynamodbDataModel()
    {
        return dynamodbDataModel;
    }

    public List<BenchmarkType> getBenchmarkTypes()
    {
        return benchmarkTypes;
    }

    public Boolean randomizedClustering()
    {
        return randomizedClustering;
    }

    public Integer getCacheValuesCount()
    {
        return cacheValuesCount;
    }

    public Double getCacheIncrementFactor()
    {
        return cacheIncrementFactor;
    }

    public List<Integer> getCachePercentages()
    {
        return cachePercentages;
    }

    public File getActualCommunitiesFile()
    {
        return actualCommunities;
    }

    public boolean permuteBenchmarks()
    {
        return permuteBenchmarks;
    }

    public int getScenarios()
    {
        return scenarios;
    }

    private static final File validateReadableFile(String fileName, String fileType) {
        File file = new File(fileName);
        if (!file.exists()) {
            throw new IllegalArgumentException(String.format("the %s does not exist", fileType));
        }

        if (!(file.isFile() && file.canRead())) {
            throw new IllegalArgumentException(String.format("the %s must be a file that this user can read", fileType));
        }
        return file;
    }

    public long getCsvReportingInterval()
    {
        return csvReportingInterval;
    }

    public long getGraphiteReportingInterval()
    {
        return graphiteReportingInterval;
    }

    public File getCsvDir()
    {
        return csvDir;
    }

    public String getGraphiteHostname()
    {
        return graphiteHostname;
    }

    public int getTitanBufferSize()
    {
        return bufferSize;
    }

    public int getTitanIdsBlocksize()
    {
        return blocksize;
    }

    public int getTitanPageSize()
    {
        return pageSize;
    }

    public int getDynamodbWorkerThreads()
    {
        return dynamodbWorkerThreads;
    }

    public boolean getDynamodbPrecreateTables()
    {
        return dynamodbPrecreateTables;
    }

    public String getDynamodbTablePrefix()
    {
        return dynamodbTablePrefix;
    }

    public boolean publishCsvMetrics()
    {
        return csvDir != null;
    }

    public boolean publishGraphiteMetrics()
    {
        return graphiteHostname != null && !graphiteHostname.isEmpty();
    }

    public boolean isCustomIds() {
        return customIds;
    }

    public long getTuplMinCacheSize() {
        return tuplMinCacheSize;
    }

    public Random getRandom() {
        return random;
    }
    public List<Integer> getRandomNodeList() {
        return DatasetFactory.getInstance().getDataset(this.dataset).getRandomNodes();
    }

    public int getShortestPathMaxHops() {
        return shortestPathMaxHops;
    }
}
