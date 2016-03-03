package eu.socialsensor.graphdatabases;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

import eu.socialsensor.insert.Insertion;
import eu.socialsensor.insert.OrientMassiveInsertion;
import eu.socialsensor.insert.OrientSingleInsertion;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.apache.tinkerpop.gremlin.orientdb.OrientVertex;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * OrientDB graph database implementation
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 */
public class OrientGraphDatabase extends GraphDatabaseBase<Iterator<Vertex>, Iterator<Edge>, Vertex, Edge>
{

    //to look up the existence of indexes in OrientDB, you need to have vertex labels.
    public static final String NODE_LABEL = "NODE";
    private OrientGraph graph = null;

    public OrientGraphDatabase(BenchmarkConfiguration config, File dbStorageDirectoryIn)
    {
        super(GraphDatabaseType.ORIENT_DB, dbStorageDirectoryIn);
        OGlobalConfiguration.STORAGE_COMPRESSION_METHOD.setValue("nothing");
    }

    @Override
    public void open()
    {
        graph = getGraph(dbStorageDirectory);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void createGraphForSingleLoad()
    {
        OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(false);
        graph = getGraph(dbStorageDirectory);
        createSchema();
    }

    @SuppressWarnings("deprecation")
    @Override
    public void createGraphForMassiveLoad()
    {
        OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(false);
        graph = getGraph(dbStorageDirectory);
        createSchema();
    }

    @Override
    public void massiveModeLoading(File dataPath)
    {
        OrientMassiveInsertion orientMassiveInsertion = new OrientMassiveInsertion(graph);
        orientMassiveInsertion.createGraph(dataPath, 0 /* scenarioNumber */);
    }

    @Override
    public void singleModeLoading(File dataPath, File resultsPath, int scenarioNumber)
    {
        Insertion orientSingleInsertion = new OrientSingleInsertion(this.graph, resultsPath);
        orientSingleInsertion.createGraph(dataPath, scenarioNumber);
    }

    @Override
    public void shutdown()
    {
        if (graph == null)
        {
            return;
        }
        try
        {
            graph.close();
        } catch(Exception e) {
            throw new IllegalStateException("unable to close graph", e);
        }
        graph = null;
    }

    @Override
    public void delete()
    {
        OrientGraph g = getGraph(dbStorageDirectory);
        g.getRawDatabase().drop();

        Utils.deleteRecursively(dbStorageDirectory);
    }

    @Override
    public void shutdownMassiveGraph()
    {
        shutdown();
    }

    @Override
    public void shortestPath(final Vertex v1, Integer i)
    {
        final OrientVertex v2 = (OrientVertex) getVertex(i);

        //TODO(amcp) need to do something about the number 5
//        List<ORID> result = (List<ORID>) new OSQLFunctionShortestPath().execute(graph,
//            null, null, new Object[] { ((OrientVertex) v1).getRecord(), v2.getRecord(), Direction.OUT, 5 },
//            new OBasicCommandContext());
//
//        result.size();
    }

    @Override
    public int getNodeCount()
    {
        return graph.traversal().V().count().toList().get(0).intValue();
    }

    @Override
    public Set<Integer> getNeighborsIds(int nodeId)
    {
        final Set<Integer> neighbours = new HashSet<Integer>();
        final Vertex vertex = getVertex(nodeId);
        vertex.vertices(Direction.IN, SIMILAR).forEachRemaining(new Consumer<Vertex>() {
            @Override
            public void accept(Vertex t) {
                Integer neighborId = (Integer) t.property(NODE_ID).value();
                neighbours.add(neighborId);
            }
        });
        return neighbours;
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
        for (Vertex v : graph.traversal().V().toList())
        {
            v.property(NODE_COMMUNITY, communityCounter);
            v.property(COMMUNITY, communityCounter);
            communityCounter++;
        }
    }

    @Override
    public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunity)
    {
        Set<Integer> communities = new HashSet<Integer>();

        for (Vertex vertex : graph.traversal().V().has(NODE_COMMUNITY, nodeCommunity).toList())
        {
            final Iterator<Vertex> it = vertex.vertices(Direction.OUT, SIMILAR);
            for (Vertex v; it.hasNext();)
            {
                v = it.next();
                int community = (Integer) v.property(COMMUNITY).value();
                if (!communities.contains(community))
                {
                    communities.add(community);
                }
            }
        }
        return communities;
    }

    @Override
    public Set<Integer> getNodesFromCommunity(int community)
    {
        Set<Integer> nodes = new HashSet<Integer>();
        for (Vertex v : graph.traversal().V().has(COMMUNITY, community).toList())
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
        for (Vertex v : graph.traversal().V().has(NODE_COMMUNITY, nodeCommunity).toList())
        {
            Integer nodeId = (Integer) v.property(NODE_ID).value();
            nodes.add(nodeId);
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

    protected void createSchema()
    {
        createIndex(NODE_ID, NODE_LABEL, "UNIQUE_HASH_INDEX", "INTEGER");
        createIndex(COMMUNITY, null /*label*/, "NOTUNIQUE_HASH_INDEX", "INTEGER");
        createIndex(NODE_COMMUNITY, null /*label*/, "NOTUNIQUE_HASH_INDEX", "INTEGER");
    }

    private void createIndex(String key, String label, String type, String keytype) {
        if(graph.getVertexIndexedKeys(label).contains(NODE_ID)) {
            return;
        }
        final Configuration nodeIdIndexConfig = new PropertiesConfiguration();
        nodeIdIndexConfig.addProperty("type", type);
        nodeIdIndexConfig.addProperty("keytype", keytype);
        graph.createVertexIndex(NODE_ID, label, nodeIdIndexConfig);
    }

    private OrientGraph getGraph(final File dbPath)
    {
        Configuration config = new PropertiesConfiguration();
        config.setProperty(OrientGraph.CONFIG_URL, "plocal:" + dbPath.getAbsolutePath());
        final OrientGraphFactory graphFactory = new OrientGraphFactory(config);
        return graphFactory.getTx();
    }

    @Override
    public boolean nodeExists(int nodeId)
    {
        return graph.traversal().V().has(NODE_ID, nodeId).hasNext();
    }

    @Override
    public Iterator<Vertex> getVertexIterator()
    {
        return graph.vertices();
    }

    @Override
    public Iterator<Edge> getNeighborsOfVertex(Vertex v)
    {
        return v.edges(Direction.BOTH, SIMILAR);
    }

    @Override
    public void cleanupVertexIterator(Iterator<Vertex> it)
    {
        // NOOP for timing
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
