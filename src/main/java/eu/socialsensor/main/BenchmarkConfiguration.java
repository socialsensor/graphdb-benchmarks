package eu.socialsensor.main;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.math3.util.CombinatoricsUtils;

import com.amazon.titan.diskstorage.dynamodb.BackendDataModel;
import com.amazon.titan.diskstorage.dynamodb.Constants;
import com.google.common.primitives.Ints;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

import eu.socialsensor.dataset.DatasetFactory;

/**
 * 
 * @author Alexander Patrikalakis
 *
 */
public class BenchmarkConfiguration
{
    // orientdb Configuration
    private static final String LIGHTWEIGHT_EDGES = "lightweight-edges";

    // Sparksee / DEX configuration
    private static final String LICENSE_KEY = "license-key";

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
    private static final String NODES_COUNT = "nodes-count";
    private static final String RANDOMIZE_CLUSTERING = "randomize-clustering";
    private static final String CACHE_VALUES = "cache-values";
    private static final String CACHE_INCREMENT_FACTOR = "cache-increment-factor";
    private static final String CACHE_VALUES_COUNT = "cache-values-count";
    private static final String PERMUTE_BENCHMARKS = "permute-benchmarks";
    private static final String RANDOM_NODES = "shortest-path-random-nodes";
    
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
    private final Boolean orientLightweightEdges;
    private final String sparkseeLicenseKey;

    // shortest path
    private final int randomNodes;

    // clustering
    private final Boolean randomizedClustering;
    private final Integer nodesCount;
    private final Integer cacheValuesCount;
    private final Double cacheIncrementFactor;
    private final List<Integer> cacheValues;
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

        Configuration orient = socialsensor.subset("orient");
        orientLightweightEdges = orient.containsKey(LIGHTWEIGHT_EDGES) ? orient.getBoolean(LIGHTWEIGHT_EDGES) : null;

        Configuration sparksee = socialsensor.subset("sparksee");
        sparkseeLicenseKey = sparksee.containsKey(LICENSE_KEY) ? sparksee.getString(LICENSE_KEY) : null;

        Configuration titan = socialsensor.subset(TITAN); //TODO(amcp) move dynamodb ns into titan
        bufferSize = titan.getInt(BUFFER_SIZE, GraphDatabaseConfiguration.BUFFER_SIZE.getDefaultValue());
        blocksize = titan.getInt(IDS_BLOCKSIZE, GraphDatabaseConfiguration.IDS_BLOCK_SIZE.getDefaultValue());
        pageSize = titan.getInt(PAGE_SIZE, GraphDatabaseConfiguration.PAGE_SIZE.getDefaultValue());

        // database storage directory
        if (!socialsensor.containsKey(DATABASE_STORAGE_DIRECTORY))
        {
            throw new IllegalArgumentException("configuration must specify database-storage-directory");
        }
        dbStorageDirectory = new File(socialsensor.getString(DATABASE_STORAGE_DIRECTORY));
        dataset = validateReadableFile(socialsensor.getString(DATASET), DATASET);

        // load the dataset
        DatasetFactory.getInstance().getDataset(dataset);

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

        randomNodes = socialsensor.getInteger(RANDOM_NODES, new Integer(100));

        if (this.benchmarkTypes.contains(BenchmarkType.CLUSTERING))
        {
            if (!socialsensor.containsKey(NODES_COUNT))
            {
                throw new IllegalArgumentException("the CW benchmark requires nodes-count integer in config");
            }
            nodesCount = socialsensor.getInt(NODES_COUNT);

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

            final boolean notGenerating = socialsensor.containsKey(CACHE_VALUES);
            if (notGenerating)
            {
                List<?> objects = socialsensor.getList(CACHE_VALUES);
                cacheValues = new ArrayList<Integer>(objects.size());
                cacheValuesCount = null;
                cacheIncrementFactor = null;
                for (Object o : objects)
                {
                    cacheValues.add(Integer.valueOf(o.toString()));
                }
            }
            else if (socialsensor.containsKey(CACHE_VALUES_COUNT) && socialsensor.containsKey(CACHE_INCREMENT_FACTOR))
            {
                cacheValues = null;
                // generate the cache values with parameters
                if (!socialsensor.containsKey(CACHE_VALUES_COUNT))
                {
                    throw new IllegalArgumentException(
                        "the CW benchmark requires cache-values-count int in config when cache-values not specified");
                }
                cacheValuesCount = socialsensor.getInt(CACHE_VALUES_COUNT);

                if (!socialsensor.containsKey(CACHE_INCREMENT_FACTOR))
                {
                    throw new IllegalArgumentException(
                        "the CW benchmark requires cache-increment-factor int in config when cache-values not specified");
                }
                cacheIncrementFactor = socialsensor.getDouble(CACHE_INCREMENT_FACTOR);
            }
            else
            {
                throw new IllegalArgumentException(
                    "when doing CW benchmark, must provide cache-values or parameters to generate them");
            }
        }
        else
        {
            randomizedClustering = null;
            nodesCount = null;
            cacheValuesCount = null;
            cacheIncrementFactor = null;
            cacheValues = null;
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

    public Integer getNodesCount()
    {
        return nodesCount;
    }

    public Integer getCacheValuesCount()
    {
        return cacheValuesCount;
    }

    public Double getCacheIncrementFactor()
    {
        return cacheIncrementFactor;
    }

    public List<Integer> getCacheValues()
    {
        return cacheValues;
    }

    public File getActualCommunitiesFile()
    {
        return actualCommunities;
    }

    public Boolean orientLightweightEdges()
    {
        return orientLightweightEdges;
    }

    public String getSparkseeLicenseKey()
    {
        return sparkseeLicenseKey;
    }

    public boolean permuteBenchmarks()
    {
        return permuteBenchmarks;
    }

    public int getScenarios()
    {
        return scenarios;
    }

    private static final File validateReadableFile(String fileName, String fileType)
    {
        File file = new File(fileName);
        if (!file.exists())
        {
            throw new IllegalArgumentException(String.format("the %s does not exist", fileType));
        }

        if (!(file.isFile() && file.canRead()))
        {
            throw new IllegalArgumentException(String.format("the %s must be a file that this user can read", fileType));
        }
        return file;
    }

    public int getRandomNodes()
    {
        return randomNodes;
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
}
