package eu.socialsensor.graphdatabases;

import java.io.File;
import java.io.IOError;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.LoopTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

import com.amazon.titan.diskstorage.dynamodb.BackendDataModel;
import com.amazon.titan.diskstorage.dynamodb.Client;
import com.amazon.titan.diskstorage.dynamodb.Constants;
import com.amazon.titan.diskstorage.dynamodb.DynamoDBSingleRowStore;
import com.amazon.titan.diskstorage.dynamodb.DynamoDBStore;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;

import com.google.bigtable.repackaged.com.google.api.client.util.Lists;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import eu.socialsensor.insert.Insertion;
import eu.socialsensor.insert.TitanMassiveInsertion;
import eu.socialsensor.insert.TitanSingleInsertion;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;

/**
 * Titan graph database implementation
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 * @author Lindsay Smith lindsaysmith@google.com
 */
public class TitanGraphDatabase extends GraphDatabaseBase<Iterator<Vertex>, Iterator<Edge>, Vertex, Edge>
{
    public static final String INSERTION_TIMES_OUTPUT_PATH = "data/titan.insertion.times";

    double totalWeight;

    private TitanGraph titanGraph;    
    public final BenchmarkConfiguration config;

    public TitanGraphDatabase(GraphDatabaseType type, BenchmarkConfiguration config, File dbStorageDirectory)
    {
        super(type, dbStorageDirectory);
        this.config = config;
        if (!GraphDatabaseType.TITAN_FLAVORS.contains(type))
        {
            throw new IllegalArgumentException(String.format("The graph database %s is not a Titan database.",
                type == null ? "null" : type.name()));
        }
    }

    @Override
    public void open()
    {
        open(false /* batchLoading */);
    }

    private static final Configuration generateBaseTitanConfiguration(GraphDatabaseType type, File dbPath,
        boolean batchLoading, BenchmarkConfiguration bench)
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
        final Configuration storage = conf.subset(GraphDatabaseConfiguration.STORAGE_NS.getName());
        final Configuration ids = conf.subset(GraphDatabaseConfiguration.IDS_NS.getName());
        final Configuration metrics = conf.subset(GraphDatabaseConfiguration.METRICS_NS.getName());

        conf.addProperty(GraphDatabaseConfiguration.ALLOW_SETTING_VERTEX_ID.getName(), "true");

        // storage NS config. FYI, storage.idauthority-wait-time is 300ms
        storage.addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND.getName(), type.getBackend());
        storage.addProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY.getName(), dbPath.getAbsolutePath());
        storage.addProperty(GraphDatabaseConfiguration.STORAGE_BATCH.getName(), Boolean.toString(batchLoading));
        storage.addProperty(GraphDatabaseConfiguration.BUFFER_SIZE.getName(), bench.getTitanBufferSize());
        storage.addProperty(GraphDatabaseConfiguration.PAGE_SIZE.getName(), bench.getTitanPageSize());

        // ids NS config
        ids.addProperty(GraphDatabaseConfiguration.IDS_BLOCK_SIZE.getName(), bench.getTitanIdsBlocksize());

        // Titan metrics - https://github.com/thinkaurelius/titan/wiki/Titan-Performance-and-Monitoring
        metrics.addProperty(GraphDatabaseConfiguration.BASIC_METRICS.getName(), "true");
        metrics.addProperty("prefix", type.getShortname());
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
        
        return conf;
    }

    private static final TitanGraph buildTitanGraph(GraphDatabaseType type, File dbPath, BenchmarkConfiguration bench,
        boolean batchLoading)
    {
        final Configuration conf = generateBaseTitanConfiguration(type, dbPath, batchLoading, bench);
        final Configuration storage = conf.subset(GraphDatabaseConfiguration.STORAGE_NS.getName());

        if (GraphDatabaseType.TITAN_CASSANDRA == type)
        {
            storage.addProperty("hostname", "localhost");
            storage.addProperty("transactions", Boolean.toString(batchLoading));
        }
        else if (GraphDatabaseType.TITAN_CASSANDRA_EMBEDDED == type)
        {
            // TODO(amcp) - this line seems broken:
            // throws: Unknown configuration element in namespace
            // [root.storage]: cassandra-config-dir
            storage.addProperty("cassandra-config-dir", "configuration/cassandra.yaml");
            storage.addProperty("transactions", Boolean.toString(batchLoading));
        }
        else if (GraphDatabaseType.TITAN_DYNAMODB == type)
        {
            final Configuration dynamodb = storage.subset("dynamodb");
            final Configuration client = dynamodb.subset(Constants.DYNAMODB_CLIENT_NAMESPACE.getName());
            final Configuration credentials = client.subset(Constants.DYNAMODB_CLIENT_CREDENTIALS_NAMESPACE.getName());
            storage.addProperty("transactions", Boolean.toString(batchLoading));
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
		else if (GraphDatabaseType.TITAN_CLOUDBIGTABLE == type)
        {          
          storage.addProperty("hbase.ext.hbase.client.connection.impl", bench.getBigtableConnectionImpl());
          storage.addProperty("hbase.ext.google.bigtable.cluster.name", bench.getBigtableClusterName());
          storage.addProperty("hbase.ext.google.bigtable.project.id", bench.getBigtableProjectId());
          storage.addProperty("hbase.ext.google.bigtable.zone.name", bench.getBigtableZone());
          
          conf.addProperty("cache.db-cache", true);         
        }
                               
        if (batchLoading) {
          storage.setProperty("batch-loading", true);
        } else {
          storage.setProperty("batch-loading", false);
        }        
        
        return TitanFactory.open(conf);
    }

    private void open(boolean batchLoading)
    {
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
        titanGraph = buildTitanGraph(type, dbStorageDirectory, config, batchLoading);
    }

    @Override
    public void createGraphForSingleLoad()
    {
        open();
        createSchema();
    }

    @Override
    public void createGraphForMassiveLoad()
    {
        open(true /* batchLoading */);
        createSchema();
    }

    @Override
    public void massiveModeLoading(File dataPath)
    {
        Insertion titanMassiveInsertion = new TitanMassiveInsertion(this.titanGraph, type, null);
        titanMassiveInsertion.createGraph(dataPath, 0 /* scenarioNumber */);
    }

    @Override
    public void singleModeLoading(File dataPath, File resultsPath, int scenarioNumber)
    {
        Insertion titanSingleInsertion = new TitanSingleInsertion(this.titanGraph, type, resultsPath);
        titanSingleInsertion.createGraph(dataPath, scenarioNumber);
    }

    @Override
    public void shutdown()
    {
        if (titanGraph == null)
        {
            return;
        }
        try
        {
            titanGraph.close();
        }
        catch (IOError e)
        {
            // TODO Fix issue in shutting down titan-cassandra-embedded
            System.err.println("Failed to shutdown titan graph: " + e.getMessage());
        }

        titanGraph = null;
    }

    @Override
    public void delete()
    {
        titanGraph = buildTitanGraph(type, dbStorageDirectory, config, false /* batchLoading */);
        try
        {
            titanGraph.close();
        }
        catch (IOError e)
        {
            // TODO Fix issue in shutting down titan-cassandra-embedded
            System.err.println("Failed to shutdown titan graph: " + e.getMessage());
        }
        TitanCleanup.clear(titanGraph);
        try
        {
            titanGraph.close();
        }
        catch (IOError e)
        {
            // TODO Fix issue in shutting down titan-cassandra-embedded
            System.err.println("Failed to shutdown titan graph: " + e.getMessage());
        }
        Utils.deleteRecursively(dbStorageDirectory);
    }

    @Override
    public void shutdownMassiveGraph()
    {
        if (titanGraph == null)
        {
            return;
        }      
        try
        {
            titanGraph.close();
        }
        catch (IOError e)
        {
            // TODO Fix issue in shutting down titan-cassandra-embedded
            System.err.println("Failed to shutdown titan graph: " + e.getMessage());
        }        
        titanGraph = null;
    }

    @Override
    public void shortestPath(final Vertex fromNode, Integer node)
    {       
        Iterator<Path> iter = titanGraph.traversal().V(fromNode)
            .repeat(out().simplePath())
            .times(4)
            .emit(has(NODE_ID,node))
            .has(NODE_ID,node)            
            .path()
            .limit(1);
        
        @SuppressWarnings("unused")
        long ix = 0;
        for (; iter.hasNext(); ++ix) {          
          System.out.println(iter.next());
        }
    }     

    @Override
    public int getNodeCount()
    {
        return (int) IteratorUtils.count(titanGraph.traversal().V());
    }

    @Override
    public Set<Integer> getNeighborsIds(int nodeId)
    {        
        Set<Integer> neighbors = new HashSet<Integer>();
        Vertex vertex = titanGraph.traversal().V().has(NODE_ID, nodeId).next();        
        Iterator<Vertex> iter = titanGraph.traversal().V(vertex).out(SIMILAR);
        while (iter.hasNext())
        {
            VertexProperty<Integer> property = iter.next().property(NODE_ID);
            Integer neighborId = property.value();
            neighbors.add(neighborId);
        }
        return neighbors;
    }

    @Override
    public double getNodeWeight(int nodeId)
    {
        Vertex vertex = titanGraph.traversal().V().has(NODE_ID, nodeId).next();
        double weight = getNodeOutDegree(vertex);
        return weight;
    }

    public double getNodeInDegree(Vertex vertex)
    {        
        return IteratorUtils.count(titanGraph.traversal().V(vertex).in(SIMILAR));
    }

    public double getNodeOutDegree(Vertex vertex)
    {        
        Iterator<Vertex> iter = titanGraph.traversal().V(vertex).out(SIMILAR);
        return IteratorUtils.count(iter);
    }

    @Override
    public void initCommunityProperty()
    {
        int communityCounter = 0;
        Iterator<Vertex> vertices = titanGraph.traversal().V();
        while (vertices.hasNext())
        {
            Vertex v = vertices.next();
            v.property(NODE_COMMUNITY, communityCounter);
            v.property(COMMUNITY, communityCounter);
            communityCounter++;
        }
    }

    @Override
    public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities)
    {
        Set<Integer> communities = new HashSet<Integer>();
        Iterator<Vertex> vertices = titanGraph.traversal().V().has(NODE_COMMUNITY, nodeCommunities);
        while (vertices.hasNext())
        {            
            Vertex vertex = vertices.next();            
            Iterator<Vertex> iter = titanGraph.traversal().V(vertex).out(SIMILAR);
            while (iter.hasNext())
            {
                VertexProperty<Integer> property = iter.next().property(COMMUNITY);
                int community = property.value();
                communities.add(community);
            }
        }
        return communities;
    }

    @Override
    public Set<Integer> getNodesFromCommunity(int community)
    {
        Set<Integer> nodes = new HashSet<Integer>();
        Iterator<Vertex> iter = titanGraph.traversal().V().has(COMMUNITY, community);
        while (iter.hasNext())
        {
          VertexProperty<Integer> property = iter.next().property(NODE_ID);
          Integer nodeId = property.value();
          nodes.add(nodeId);
        }
        return nodes;
    }

    @Override
    public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity)
    {
        Set<Integer> nodes = new HashSet<Integer>();
        GraphTraversal<Vertex, Vertex> iter = titanGraph.traversal().V().has(NODE_COMMUNITY, nodeCommunity);
        while (iter.hasNext())
        {
            VertexProperty<Integer> property = iter.next().property(NODE_ID);
            Integer nodeId = property.value();
            nodes.add(nodeId);
        }
        return nodes;
    }

    @Override
    public double getEdgesInsideCommunity(int vertexCommunity, int communityVertices)
    {
        double edges = 0;
        Iterator<Vertex> vertices = titanGraph.traversal().V().has(NODE_COMMUNITY, vertexCommunity);
        List<Vertex> comVertices = IteratorUtils.asList(titanGraph.traversal().V().has(COMMUNITY, communityVertices));        
        while (vertices.hasNext())
        {
            Iterator<Vertex> neighbours = vertices.next().vertices(Direction.OUT, SIMILAR); 
            while (neighbours.hasNext())
            {
                if (Iterables.contains(comVertices, neighbours.next()))
                {
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
        Iterator<Vertex> iter = titanGraph.traversal().V().has(COMMUNITY, community);
        while (iter.hasNext())      
        {
            communityWeight += getNodeOutDegree(iter.next());
        }        
        return communityWeight;
    }

    @Override
    public double getNodeCommunityWeight(int nodeCommunity)
    {
        double nodeCommunityWeight = 0;
        Iterator<Vertex> iter = titanGraph.traversal().V().has(NODE_COMMUNITY, nodeCommunity);
        while (iter.hasNext())
        {
            nodeCommunityWeight += getNodeOutDegree(iter.next());
        }
        return nodeCommunityWeight;
    }

    @Override
    public void moveNode(int nodeCommunity, int toCommunity)
    {
        Iterator<Vertex> fromIter = titanGraph.traversal().V().has(NODE_COMMUNITY, nodeCommunity);
        while (fromIter.hasNext())
        {
            fromIter.next().property(COMMUNITY, toCommunity);
        }
    }

    @Override
    public double getGraphWeightSum()
    {
        Iterator<Edge> edges = titanGraph.edges();
        return IteratorUtils.count(edges);
    }

    @Override
    public int reInitializeCommunities()
    {
        Map<Integer, Integer> initCommunities = new HashMap<Integer, Integer>();
        int communityCounter = 0;
        Iterator<Vertex> vertices = titanGraph.traversal().V();
        while (vertices.hasNext())
        {
            Vertex v = vertices.next();
            VertexProperty<Integer> property = v.property(COMMUNITY);
            int communityId = property.value();;
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
        Vertex vertex = titanGraph.traversal().V().has(NODE_COMMUNITY, nodeCommunity).next();
        VertexProperty<Integer> property = vertex.property(COMMUNITY);
        return property.value();
    }

    @Override
    public int getCommunityFromNode(int nodeId)
    {
        Vertex vertex = titanGraph.traversal().V().has(NODE_ID, nodeId).next();
        VertexProperty<Integer> property = vertex.property(COMMUNITY);
        return property.value();
    }

    @Override
    public int getCommunitySize(int community)
    {
        Iterator<Vertex> vertices = titanGraph.traversal().V().has(COMMUNITY, community);
        Set<Integer> nodeCommunities = new HashSet<Integer>();
        while (vertices.hasNext())
        {
            VertexProperty<Integer> property = vertices.next().property(NODE_COMMUNITY);
            int nodeCommunity = property.value();
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
            Iterator<Vertex> verticesIter = titanGraph.traversal().V().has(COMMUNITY, i);
            List<Integer> vertices = new ArrayList<Integer>();
            while (verticesIter.hasNext())
            {
                VertexProperty<Integer> property = verticesIter.next().property(NODE_ID);
                Integer nodeId = property.value();
                vertices.add(nodeId);
            }
            communities.put(i, vertices);
        }
        return communities;
    }

    private void createSchema()
    {
        final TitanManagement mgmt = titanGraph.openManagement();
        if (!mgmt.containsGraphIndex(NODE_ID))
        {
            final PropertyKey key = mgmt.makePropertyKey(NODE_ID).dataType(Integer.class).make();
            mgmt.buildIndex(NODE_ID, Vertex.class).addKey(key).unique().buildCompositeIndex();            
        }
        if (!mgmt.containsGraphIndex(COMMUNITY))
        {
            final PropertyKey key = mgmt.makePropertyKey(COMMUNITY).dataType(Integer.class).make();
            mgmt.buildIndex(COMMUNITY, Vertex.class).addKey(key).buildCompositeIndex();
        }
        if (!mgmt.containsGraphIndex(NODE_COMMUNITY))
        {
            final PropertyKey key = mgmt.makePropertyKey(NODE_COMMUNITY).dataType(Integer.class).make();
            mgmt.buildIndex(NODE_COMMUNITY, Vertex.class).addKey(key).buildCompositeIndex();
        }

        if (mgmt.getEdgeLabel(SIMILAR) == null)
        {
            mgmt.makeEdgeLabel(SIMILAR).multiplicity(Multiplicity.MULTI).directed().make();
        }
        mgmt.commit();
    }

    @Override
    public boolean nodeExists(int nodeId)
    {
        return titanGraph.traversal().V().has(NODE_ID, nodeId).hasNext();        
    }

    @Override
    public Iterator<Vertex> getVertexIterator()
    {
        return titanGraph.traversal().V();
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
        return titanGraph.traversal().E();
    }

    @Override
    public Vertex getSrcVertexFromEdge(Edge edge)
    {
        return edge.inVertex();
    }

    @Override
    public Vertex getDestVertexFromEdge(Edge edge)
    {
        return edge.outVertex();
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
        return titanGraph.traversal().V().has(NODE_ID, i.intValue()).next();
    }
}
