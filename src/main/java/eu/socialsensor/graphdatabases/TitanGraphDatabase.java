package eu.socialsensor.graphdatabases;

import java.io.File;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.amazon.titan.diskstorage.dynamodb.BackendDataModel;
import com.amazon.titan.diskstorage.dynamodb.Client;
import com.amazon.titan.diskstorage.dynamodb.Constants;
import com.amazon.titan.diskstorage.dynamodb.DynamoDBSingleRowStore;
import com.amazon.titan.diskstorage.dynamodb.DynamoDBStore;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.core.schema.VertexLabelMaker;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;

import eu.socialsensor.insert.Insertion;
import eu.socialsensor.insert.TitanMassiveInsertion;
import eu.socialsensor.insert.TitanSingleInsertion;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;
import jp.classmethod.titan.diskstorage.tupl.TuplStoreManager;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource.computer;

/**
 * Titan graph database implementation
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 */
public class TitanGraphDatabase extends GraphDatabaseBase<Iterator<Vertex>, Iterator<Edge>, Vertex, Edge>
{
    private static final Logger LOG = LogManager.getLogger();
    public static final String INSERTION_TIMES_OUTPUT_PATH = "data/titan.insertion.times";

    double totalWeight;

    private final StandardTitanGraph graph;
    private final BenchmarkConfiguration config;

    public TitanGraphDatabase(GraphDatabaseType type, BenchmarkConfiguration config, File dbStorageDirectory,
            boolean batchLoading)
    {
        super(type, dbStorageDirectory);
        this.config = config;
        if (!GraphDatabaseType.TITAN_FLAVORS.contains(type))
        {
            throw new IllegalArgumentException(String.format("The graph database %s is not a Titan database.",
                type == null ? "null" : type.name()));
        }
        graph = open(batchLoading);
        createSchema();
    }

    private static final StandardTitanGraph buildTitanGraph(GraphDatabaseType type, File dbPath, BenchmarkConfiguration bench,
        boolean batchLoading)
    {
        if (!GraphDatabaseType.TITAN_FLAVORS.contains(type))
        {
            throw new IllegalArgumentException("must provide a Titan database type but got "
                + (type == null ? "null" : type.name()));
        }

        if (dbPath == null)
        {
            throw new IllegalArgumentException("the dbPath must not be null");
        }
        if (!dbPath.exists() || !dbPath.canWrite() || !dbPath.isDirectory())
        {
            throw new IllegalArgumentException("db path must exist as a directory and must be writeable");
        }
        final Configuration conf = new MapConfiguration(new HashMap<String, String>());
        final Configuration graph = conf.subset(GraphDatabaseConfiguration.GRAPH_NS.getName());
        final Configuration storage = conf.subset(GraphDatabaseConfiguration.STORAGE_NS.getName());
        final Configuration ids = conf.subset(GraphDatabaseConfiguration.IDS_NS.getName());
        final Configuration metrics = conf.subset(GraphDatabaseConfiguration.METRICS_NS.getName());
        final Configuration cluster = conf.subset(GraphDatabaseConfiguration.CLUSTER_NS.getName());

        //graph NS config
        if(bench.isCustomIds()) {
            //TODO(amcp) figure out a way to claim the ids used for this unique-instance-id
            graph.addProperty(GraphDatabaseConfiguration.ALLOW_SETTING_VERTEX_ID.getName(), "true");
        }
        graph.addProperty(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID.getName(), "DEADBEEF");

        //cluster NS config. only two partitions for now
        //recall the number of partitions is a FIXED property so user cant override
        //initial value stored in system_properties the first time the graph is loaded.
        //default is 32
        cluster.addProperty(GraphDatabaseConfiguration.CLUSTER_MAX_PARTITIONS.getName(), 2);

        // storage NS config. FYI, storage.idauthority-wait-time is 300ms
        storage.addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND.getName(), type.getBackend());
        storage.addProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY.getName(), dbPath.getAbsolutePath());
        storage.addProperty(GraphDatabaseConfiguration.STORAGE_BATCH.getName(), Boolean.toString(batchLoading));
        storage.addProperty(GraphDatabaseConfiguration.STORAGE_TRANSACTIONAL.getName(), Boolean.toString(!batchLoading));
        storage.addProperty(GraphDatabaseConfiguration.BUFFER_SIZE.getName(), bench.getTitanBufferSize());
        storage.addProperty(GraphDatabaseConfiguration.PAGE_SIZE.getName(), bench.getTitanPageSize());
        storage.addProperty(GraphDatabaseConfiguration.PARALLEL_BACKEND_OPS.getName(), "true");

        // ids NS config
        ids.addProperty(GraphDatabaseConfiguration.IDS_BLOCK_SIZE.getName(), bench.getTitanIdsBlocksize());

        // Titan metrics - https://github.com/thinkaurelius/titan/wiki/Titan-Performance-and-Monitoring
        boolean anyMetrics = bench.publishGraphiteMetrics() || bench.publishCsvMetrics();
        if(anyMetrics) {
            metrics.addProperty(GraphDatabaseConfiguration.BASIC_METRICS.getName(), anyMetrics);
            metrics.addProperty("prefix", type.getShortname());
        }
        if(bench.publishGraphiteMetrics()) {
            final Configuration graphite = metrics.subset(BenchmarkConfiguration.GRAPHITE);
            graphite.addProperty("hostname", bench.getGraphiteHostname());
            graphite.addProperty(BenchmarkConfiguration.CSV_INTERVAL, bench.getCsvReportingInterval());
        }
        if(bench.publishCsvMetrics()) {
            final Configuration csv = metrics.subset(GraphDatabaseConfiguration.METRICS_CSV_NS.getName());
            csv.addProperty(GraphDatabaseConfiguration.METRICS_CSV_DIR.getName(), bench.getCsvDir().getAbsolutePath());
            csv.addProperty(BenchmarkConfiguration.CSV_INTERVAL, bench.getCsvReportingInterval());
        }

        if (GraphDatabaseType.TITAN_CASSANDRA == type)
        {
            storage.addProperty(GraphDatabaseConfiguration.STORAGE_HOSTS.getName(),
                    "localhost");
        }
        else if (GraphDatabaseType.TITAN_TUPL == type)
        {
            final Configuration tupl = storage.subset(TuplStoreManager.TUPL_NS.getName());
            tupl.addProperty(TuplStoreManager.TUPL_PREFIX.getName(), "tupldb");
            tupl.addProperty(TuplStoreManager.TUPL_DIRECT_PAGE_ACCESS.getName(), Boolean.TRUE.toString());
            tupl.addProperty(TuplStoreManager.TUPL_MIN_CACHE_SIZE.getName(), Long.toString(bench.getTuplMinCacheSize()));
            tupl.addProperty(TuplStoreManager.TUPL_MAP_DATA_FILES.getName(), Boolean.TRUE.toString());
            final Configuration checkpoint = tupl.subset(TuplStoreManager.TUPL_CHECKPOINT_NS.getName());
            //TODO make this conditioned on running the Massive Insertion Workload
            //checkpoint.addProperty(TuplStoreManager.TUPL_CHECKPOINT_SIZE_THRESHOLD.getName(), 0);
        }
        else if (GraphDatabaseType.TITAN_DYNAMODB == type)
        {
            final Configuration dynamodb = storage.subset("dynamodb");
            final Configuration client = dynamodb.subset(Constants.DYNAMODB_CLIENT_NAMESPACE.getName());
            final Configuration credentials = client.subset(Constants.DYNAMODB_CLIENT_CREDENTIALS_NAMESPACE.getName());
            if (bench.getDynamodbDataModel() == null)
            {
                throw new IllegalArgumentException("data model must be set for dynamodb benchmarking");
            }
            if (GraphDatabaseType.TITAN_DYNAMODB == type && bench.getDynamodbEndpoint() != null
                && !bench.getDynamodbEndpoint().isEmpty())
            {
                client.addProperty(Constants.DYNAMODB_CLIENT_ENDPOINT.getName(), bench.getDynamodbEndpoint());
                client.addProperty(Constants.DYNAMODB_CLIENT_MAX_CONN.getName(), bench.getDynamodbWorkerThreads());
            } else {
                throw new IllegalArgumentException("require endpoint");
            }

            if (bench.getDynamodbCredentialsFqClassName() != null
                && !bench.getDynamodbCredentialsFqClassName().isEmpty())
            {
                credentials.addProperty(Constants.DYNAMODB_CREDENTIALS_CLASS_NAME.getName(), bench.getDynamodbCredentialsFqClassName());
            }

            if (bench.getDynamodbCredentialsCtorArguments() != null)
            {
                credentials.addProperty(Constants.DYNAMODB_CREDENTIALS_CONSTRUCTOR_ARGS.getName(),
                    bench.getDynamodbCredentialsCtorArguments());
            }

            dynamodb.addProperty(Constants.DYNAMODB_FORCE_CONSISTENT_READ.getName(), bench.dynamodbConsistentRead());
            Configuration executor = client.subset(Constants.DYNAMODB_CLIENT_EXECUTOR_NAMESPACE.getName());
            executor.addProperty(Constants.DYNAMODB_CLIENT_EXECUTOR_CORE_POOL_SIZE.getName(), bench.getDynamodbWorkerThreads());
            executor.addProperty(Constants.DYNAMODB_CLIENT_EXECUTOR_MAX_POOL_SIZE.getName(), bench.getDynamodbWorkerThreads());
            executor.addProperty(Constants.DYNAMODB_CLIENT_EXECUTOR_KEEP_ALIVE.getName(), TimeUnit.MINUTES.toMillis(1));
            executor.addProperty(Constants.DYNAMODB_CLIENT_EXECUTOR_QUEUE_MAX_LENGTH.getName(), bench.getTitanBufferSize());

            final long writeTps = bench.getDynamodbTps();
            final long readTps = Math.max(1, bench.dynamodbConsistentRead() ? writeTps : writeTps / 2);

            final Configuration stores = dynamodb.subset(Constants.DYNAMODB_STORES_NAMESPACE.getName());
            for (String storeName : Constants.REQUIRED_BACKEND_STORES)
            {
                final Configuration store = stores.subset(storeName);
                store.addProperty(Constants.STORES_DATA_MODEL.getName(), bench.getDynamodbDataModel().name());
                store.addProperty(Constants.STORES_CAPACITY_READ.getName(), readTps);
                store.addProperty(Constants.STORES_CAPACITY_WRITE.getName(), writeTps);
                store.addProperty(Constants.STORES_READ_RATE_LIMIT.getName(), readTps);
                store.addProperty(Constants.STORES_WRITE_RATE_LIMIT.getName(), writeTps);
            }
        }
        return (StandardTitanGraph) TitanFactory.open(conf);
    }

    private StandardTitanGraph open(boolean batchLoading)
    {
        //if using DynamoDB Storage Backend for Titan, prep the tables in parallel
        if(type == GraphDatabaseType.TITAN_DYNAMODB && config.getDynamodbPrecreateTables()) {
            List<CreateTableRequest> requests = new LinkedList<>();
            long wcu = config.getDynamodbTps();
            long rcu = Math.max(1, config.dynamodbConsistentRead() ? wcu : (wcu / 2));
            for(String store : Constants.REQUIRED_BACKEND_STORES) {
                final String tableName = config.getDynamodbTablePrefix() + "_" + store;
                if(BackendDataModel.MULTI == config.getDynamodbDataModel()) {
                    requests.add(DynamoDBStore.createTableRequest(tableName,
                        rcu, wcu));
                } else if(BackendDataModel.SINGLE == config.getDynamodbDataModel()) {
                    requests.add(DynamoDBSingleRowStore.createTableRequest(tableName, rcu, wcu));
                }
            }
            //TODO is this autocloseable?
            final AmazonDynamoDB client =
                new AmazonDynamoDBClient(Client.createAWSCredentialsProvider(config.getDynamodbCredentialsFqClassName(),
                    config.getDynamodbCredentialsCtorArguments() == null ? null : config.getDynamodbCredentialsCtorArguments().split(",")));
            client.setEndpoint(config.getDynamodbEndpoint());
            for(CreateTableRequest request : requests) {
                try {
                    client.createTable(request);
                } catch(ResourceInUseException ignore) {
                    //already created, good
                }
            }
            client.shutdown();
        }
        return buildTitanGraph(type, dbStorageDirectory, config, batchLoading);
    }

    @Override
    public void massiveModeLoading(File dataPath)
    {
        Insertion titanMassiveInsertion = TitanMassiveInsertion.create(graph, type, config.isCustomIds());
        titanMassiveInsertion.createGraph(dataPath, 0 /* scenarioNumber */);
        //TODO(amcp) figure out a way to claim the ids used for this unique-instance-id
    }

    @Override
    public void singleModeLoading(File dataPath, File resultsPath, int scenarioNumber)
    {
        Insertion titanSingleInsertion = new TitanSingleInsertion(this.graph, type, resultsPath);
        titanSingleInsertion.createGraph(dataPath, scenarioNumber);
    }

    @Override
    public void shutdown()
    {
        graph.close();
    }

    @Override
    public void delete()
    {
        shutdown();
        TitanCleanup.clear(graph);
        Utils.deleteRecursively(dbStorageDirectory);
    }

    @Override
    public void shutdownMassiveGraph()
    {
        shutdown();
    }

    @Override
    public void shortestPath(final Vertex fromNode, Integer targetNode)
    {
        final GraphTraversalSource g = graph.traversal();
        final Stopwatch watch = Stopwatch.createStarted();
        //            repeat the contained traversal
        //                   map from this vertex to inV on SIMILAR edges without looping
        //            until you map to the target toNode and the path is six vertices long or less
        //            only return one path
//g.V().has("nodeId", 775).repeat(out('similar').simplePath()).until(has('nodeId', 990).and().filter {it.path().size() <= 5}).limit(1).path().by('nodeId')
        GraphTraversal<?, Path> t =
        g.V().has(NODE_ID, fromNode.<Integer>value(NODE_ID))
                .repeat(
                        __.out(SIMILAR)
                                .simplePath())
                .until(
                        __.has(NODE_ID, targetNode)
                        .and(
                                __.filter(it -> {
//when the size of the path in the traverser object is six, that means this traverser made 4 hops from the
//fromNode, a total of 5 vertices
                                    return it.path().size() <= 5;
                                }))
                )
                .limit(1)
                .path();

        t.tryNext()
                .ifPresent( it -> {
                    final int pathSize = it.size();
                    final long elapsed = watch.elapsed(TimeUnit.MILLISECONDS);
                    watch.stop();
                    if(elapsed > 200) { //threshold for debugging
                        LOG.info("from @ " + fromNode.value(NODE_ID) +
                                " to @ " + targetNode.toString() +
                                " took " + elapsed + " ms, " + pathSize + ": " + it.toString());
                    }
        });


    }

    @Override
    public int getNodeCount()
    {
        final GraphTraversalSource g = graph.traversal();
        final long nodeCount = g.V().count().toList().get(0);
        return (int) nodeCount;
    }

    @Override
    public Set<Integer> getNeighborsIds(int nodeId)
    {
        final Vertex vertex = getVertex(nodeId);
        Set<Integer> neighbors = new HashSet<Integer>();
        Iterator<Vertex> iter = vertex.vertices(Direction.OUT, SIMILAR);
        while (iter.hasNext())
        {
            Integer neighborId = Integer.valueOf(iter.next().property(NODE_ID).value().toString());
            neighbors.add(neighborId);
        }
        return neighbors;
    }

    @Override
    public double getNodeWeight(int nodeId)
    {
        Vertex vertex = getVertex(nodeId);
        double weight = getNodeOutDegree(vertex);
        return weight;
    }

    public double getNodeInDegree(Vertex vertex)
    {
        return (double) Iterators.size(vertex.edges(Direction.IN, SIMILAR));
    }

    public double getNodeOutDegree(Vertex vertex)
    {
        return (double) Iterators.size(vertex.edges(Direction.OUT, SIMILAR));
    }

    @Override
    public void initCommunityProperty()
    {
        int communityCounter = 0;
        for (Vertex v : graph.traversal().V(T.label, NODE_LABEL).toList())
        {
            v.property(NODE_COMMUNITY, communityCounter);
            v.property(COMMUNITY, communityCounter);
            communityCounter++;
        }
    }

    @Override
    public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities)
    {
        Set<Integer> communities = new HashSet<Integer>();
        final GraphTraversalSource g = graph.traversal();

        for (Property<?> p : g.V().has(NODE_COMMUNITY, nodeCommunities).out(SIMILAR).properties(COMMUNITY).toSet())
        {
            communities.add((Integer) p.value());
        }
        return communities;
    }

    @Override
    public Set<Integer> getNodesFromCommunity(int community)
    {
        final GraphTraversalSource g = graph.traversal();
        Set<Integer> nodes = new HashSet<Integer>();
        for (Vertex v : g.V().has(COMMUNITY, community).toList())
        {
            Integer nodeId = (Integer) v.property(NODE_ID).value();
            nodes.add(nodeId);
        }
        return nodes;
    }

    @Override
    public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity)
    {
        Set<Integer> nodes = new HashSet<Integer>();
        final GraphTraversalSource g = graph.traversal();
        
        for (Property<?> property : g.V().has(NODE_COMMUNITY, nodeCommunity).properties(NODE_ID).toList())
        {
            nodes.add((Integer) property.value());
        }
        return nodes;
    }

    @Override
    public double getEdgesInsideCommunity(int vertexCommunity, int communityVertices)
    {
        double edges = 0;
        Set<Vertex> comVertices = graph.traversal().V().has(COMMUNITY, communityVertices).toSet();
        for (Vertex vertex : graph.traversal().V().has(NODE_COMMUNITY, vertexCommunity).toList())
        {
            Iterator<Vertex> it = vertex.vertices(Direction.OUT, SIMILAR);
            for (Vertex v; it.hasNext();)
            {
                v = it.next();
                if(comVertices.contains(v)) {
                    edges++;
                }
            }
        }
        return edges;
    }

    @Override
    public double getCommunityWeight(int community)
    {
        double communityWeight = 0;
        final List<Vertex> list = graph.traversal().V().has(COMMUNITY, community).toList();
        if (list.size() <= 1) {
            return communityWeight;
        }
        for (Vertex vertex : list)
        {
            communityWeight += getNodeOutDegree(vertex);
        }
        return communityWeight;
    }

    @Override
    public double getNodeCommunityWeight(int nodeCommunity)
    {
        double nodeCommunityWeight = 0;
        for (Vertex vertex : graph.traversal().V().has(NODE_COMMUNITY, nodeCommunity).toList())
        {
            nodeCommunityWeight += getNodeOutDegree(vertex);
        }
        return nodeCommunityWeight;
    }

    @Override
    public void moveNode(int nodeCommunity, int toCommunity)
    {
        for (Vertex vertex : graph.traversal().V().has(NODE_COMMUNITY, nodeCommunity).toList())
        {
            vertex.property(COMMUNITY, toCommunity);
        }
    }

    @Override
    public double getGraphWeightSum()
    {
        final Iterator<Edge> edges = graph.edges();
        return (double) Iterators.size(edges);
    }

    @Override
    public int reInitializeCommunities()
    {
        Map<Integer, Integer> initCommunities = new HashMap<Integer, Integer>();
        int communityCounter = 0;
        Iterator<Vertex> it = graph.vertices();
        for (Vertex v; it.hasNext();)
        {
            v = it.next();
            int communityId = (Integer) v.property(COMMUNITY).value();
            if (!initCommunities.containsKey(communityId))
            {
                initCommunities.put(communityId, communityCounter);
                communityCounter++;
            }
            int newCommunityId = initCommunities.get(communityId);
            v.property(COMMUNITY, newCommunityId);
            v.property(NODE_COMMUNITY, newCommunityId);
        }
        return communityCounter;
    }

    @Override
    public int getCommunity(int nodeCommunity)
    {
        Vertex vertex = graph.traversal().V().has(NODE_COMMUNITY, nodeCommunity).next();
        int community = (Integer) vertex.property(COMMUNITY).value();
        return community;
    }

    @Override
    public int getCommunityFromNode(int nodeId)
    {
        Vertex vertex = getVertex(nodeId);
        return (Integer) vertex.property(COMMUNITY).value();
    }

    @Override
    public int getCommunitySize(int community)
    {
        Set<Integer> nodeCommunities = new HashSet<Integer>();
        for (Vertex v : graph.traversal().V().has(COMMUNITY, community).toList())
        {
            int nodeCommunity = (Integer) v.property(NODE_COMMUNITY).value();
            if (!nodeCommunities.contains(nodeCommunity))
            {
                nodeCommunities.add(nodeCommunity);
            }
        }
        return nodeCommunities.size();
    }

    @Override
    public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities)
    {
        Map<Integer, List<Integer>> communities = new HashMap<Integer, List<Integer>>();
        for (int i = 0; i < numberOfCommunities; i++)
        {
            GraphTraversal<Vertex, Vertex> t = graph.traversal().V().has(COMMUNITY, i);
            List<Integer> vertices = new ArrayList<Integer>();
            while (t.hasNext())
            {
                Integer nodeId = (Integer) t.next().property(NODE_ID).value();
                vertices.add(nodeId);
            }
            communities.put(i, vertices);
        }
        return communities;
    }

    private void createSchema()
    {
        final TitanManagement mgmt = graph.openManagement();
        if(!mgmt.containsVertexLabel(NODE_LABEL)) {
            final VertexLabelMaker maker = mgmt.makeVertexLabel(NODE_LABEL);
            maker.make();
        }
        if (null == mgmt.getGraphIndex(NODE_ID))
        {
            final PropertyKey key = mgmt.makePropertyKey(NODE_ID).dataType(Integer.class).make();
            mgmt.buildIndex(NODE_ID, Vertex.class).addKey(key).unique().buildCompositeIndex();
        }
        if (null == mgmt.getGraphIndex(COMMUNITY))
        {
            final PropertyKey key = mgmt.makePropertyKey(COMMUNITY).dataType(Integer.class).make();
            mgmt.buildIndex(COMMUNITY, Vertex.class).addKey(key).buildCompositeIndex();
        }
        if (null == mgmt.getGraphIndex(NODE_COMMUNITY))
        {
            final PropertyKey key = mgmt.makePropertyKey(NODE_COMMUNITY).dataType(Integer.class).make();
            mgmt.buildIndex(NODE_COMMUNITY, Vertex.class).addKey(key).buildCompositeIndex();
        }
        if (mgmt.getEdgeLabel(SIMILAR) == null)
        {
            mgmt.makeEdgeLabel(SIMILAR).multiplicity(Multiplicity.MULTI).directed().make();
        }
        mgmt.commit();
        graph.tx().commit();
    }

    @Override
    public Iterator<Vertex> getVertexIterator()
    {
        return graph.traversal().V().hasLabel(NODE_LABEL).toStream().iterator();
    }

    @Override
    public Iterator<Edge> getNeighborsOfVertex(Vertex v)
    {
        return v.edges(Direction.BOTH, SIMILAR);
    }

    @Override
    public void cleanupVertexIterator(Iterator<Vertex> it)
    {
        return; // NOOP - do nothing
    }

    @Override
    public Vertex getOtherVertexFromEdge(Edge edge, Vertex oneVertex)
    {
        return edge.inVertex().equals(oneVertex) ? edge.outVertex() : edge.inVertex();
    }

    @Override
    public Iterator<Edge> getAllEdges()
    {
        return graph.edges();
    }

    @Override
    public Vertex getSrcVertexFromEdge(Edge edge)
    {
        return edge.outVertex();
    }

    @Override
    public Vertex getDestVertexFromEdge(Edge edge)
    {
        return edge.inVertex();
    }

    @Override
    public boolean edgeIteratorHasNext(Iterator<Edge> it)
    {
        return it.hasNext();
    }

    @Override
    public Edge nextEdge(Iterator<Edge> it)
    {
        return it.next();
    }

    @Override
    public void cleanupEdgeIterator(Iterator<Edge> it)
    {
        // NOOP
    }

    @Override
    public boolean vertexIteratorHasNext(Iterator<Vertex> it)
    {
        return it.hasNext();
    }

    @Override
    public Vertex nextVertex(Iterator<Vertex> it)
    {
        return it.next();
    }

    @Override
    public Vertex getVertex(Integer i)
    {
        final GraphTraversalSource g = graph.traversal();
        final Vertex vertex = g.V().has(NODE_ID, i).next();
        return vertex;
    }
}
