package eu.socialsensor.graphdatabases;

import com.google.common.collect.Iterables;
import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.graph.sql.functions.OSQLFunctionShortestPath;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

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

/**
 * OrientDB graph database implementation
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 */
public class OrientGraphDatabase extends GraphDatabaseBase<Iterator<Vertex>, Iterator<Edge>, Vertex, Edge>
{

    private OrientGraph graph = null;
    private boolean useLightWeightEdges;

    //
    public OrientGraphDatabase(BenchmarkConfiguration config, File dbStorageDirectoryIn)
    {
        super(GraphDatabaseType.ORIENT_DB, dbStorageDirectoryIn);
        OGlobalConfiguration.STORAGE_COMPRESSION_METHOD.setValue("nothing");
        this.useLightWeightEdges = config.orientLightweightEdges() == null ? true : config.orientLightweightEdges()
            .booleanValue();
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
        OrientMassiveInsertion orientMassiveInsertion = new OrientMassiveInsertion(this.graph.getRawGraph().getURL());
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
        graph.shutdown();
        graph = null;
    }

    @Override
    public void delete()
    {
        OrientGraphNoTx g = new OrientGraphNoTx("plocal:" + dbStorageDirectory.getAbsolutePath());
        g.drop();

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

        List<ORID> result = new OSQLFunctionShortestPath().execute(graph,
            null, null, new Object[] { ((OrientVertex) v1).getRecord(), v2.getRecord(), Direction.OUT, 5 },
            new OBasicCommandContext());

        result.size();
    }

    @Override
    public int getNodeCount()
    {
        return (int) graph.countVertices();
    }

    @Override
    public Set<Integer> getNeighborsIds(int nodeId)
    {
        Set<Integer> neighbours = new HashSet<Integer>();
        Vertex vertex = graph.getVertices(NODE_ID, nodeId).iterator().next();
        for (Vertex v : vertex.getVertices(Direction.IN, SIMILAR))
        {
            Integer neighborId = v.getProperty(NODE_ID);
            neighbours.add(neighborId);
        }
        return neighbours;
    }

    @Override
    public double getNodeWeight(int nodeId)
    {
        Vertex vertex = graph.getVertices(NODE_ID, nodeId).iterator().next();
        double weight = getNodeOutDegree(vertex);
        return weight;
    }

    public double getNodeInDegree(Vertex vertex)
    {
        @SuppressWarnings("rawtypes")
        OMultiCollectionIterator result = (OMultiCollectionIterator) vertex.getVertices(Direction.IN, SIMILAR);
        return (double) result.size();
    }

    public double getNodeOutDegree(Vertex vertex)
    {
        @SuppressWarnings("rawtypes")
        OMultiCollectionIterator result = (OMultiCollectionIterator) vertex.getVertices(Direction.OUT, SIMILAR);
        return (double) result.size();
    }

    @Override
    public void initCommunityProperty()
    {
        int communityCounter = 0;
        for (Vertex v : graph.getVertices())
        {
            ((OrientVertex) v).setProperties(NODE_COMMUNITY, communityCounter, COMMUNITY, communityCounter);
            ((OrientVertex) v).save();
            communityCounter++;
        }
    }

    @Override
    public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities)
    {
        Set<Integer> communities = new HashSet<Integer>();
        Iterable<Vertex> vertices = graph.getVertices(NODE_COMMUNITY, nodeCommunities);
        for (Vertex vertex : vertices)
        {
            for (Vertex v : vertex.getVertices(Direction.OUT, SIMILAR))
            {
                int community = v.getProperty(COMMUNITY);
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
        Iterable<Vertex> iter = graph.getVertices(COMMUNITY, community);
        for (Vertex v : iter)
        {
            Integer nodeId = v.getProperty(NODE_ID);
            nodes.add(nodeId);
        }
        return nodes;
    }

    @Override
    public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity)
    {
        Set<Integer> nodes = new HashSet<Integer>();
        Iterable<Vertex> iter = graph.getVertices("nodeCommunity", nodeCommunity);
        for (Vertex v : iter)
        {
            Integer nodeId = v.getProperty(NODE_ID);
            nodes.add(nodeId);
        }
        return nodes;
    }

    @Override
    public double getEdgesInsideCommunity(int vertexCommunity, int communityVertices)
    {
        double edges = 0;
        Iterable<Vertex> vertices = graph.getVertices(NODE_COMMUNITY, vertexCommunity);
        Iterable<Vertex> comVertices = graph.getVertices(COMMUNITY, communityVertices);
        for (Vertex vertex : vertices)
        {
            for (Vertex v : vertex.getVertices(Direction.OUT, SIMILAR))
            {
                if (Iterables.contains(comVertices, v))
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
        Iterable<Vertex> iter = graph.getVertices(COMMUNITY, community);
        if (Iterables.size(iter) > 1)
        {
            for (Vertex vertex : iter)
            {
                communityWeight += getNodeOutDegree(vertex);
            }
        }
        return communityWeight;
    }

    @Override
    public double getNodeCommunityWeight(int nodeCommunity)
    {
        double nodeCommunityWeight = 0;
        Iterable<Vertex> iter = graph.getVertices(NODE_COMMUNITY, nodeCommunity);
        for (Vertex vertex : iter)
        {
            nodeCommunityWeight += getNodeOutDegree(vertex);
        }
        return nodeCommunityWeight;
    }

    @Override
    public void moveNode(int nodeCommunity, int toCommunity)
    {
        Iterable<Vertex> fromIter = graph.getVertices(NODE_COMMUNITY, nodeCommunity);
        for (Vertex vertex : fromIter)
        {
            vertex.setProperty(COMMUNITY, toCommunity);
        }
    }

    @Override
    public double getGraphWeightSum()
    {
        long edges = 0;
        for (Vertex o : graph.getVertices())
        {
            edges += ((OrientVertex) o).countEdges(Direction.OUT, SIMILAR);
        }
        return (double) edges;
    }

    @Override
    public int reInitializeCommunities()
    {
        Map<Integer, Integer> initCommunities = new HashMap<Integer, Integer>();
        int communityCounter = 0;
        for (Vertex v : graph.getVertices())
        {
            int communityId = v.getProperty(COMMUNITY);
            if (!initCommunities.containsKey(communityId))
            {
                initCommunities.put(communityId, communityCounter);
                communityCounter++;
            }
            int newCommunityId = initCommunities.get(communityId);
            ((OrientVertex) v).setProperties(COMMUNITY, newCommunityId, NODE_COMMUNITY, newCommunityId);
            ((OrientVertex) v).save();
        }
        return communityCounter;
    }

    @Override
    public int getCommunity(int nodeCommunity)
    {
        final Iterator<Vertex> result = graph.getVertices(NODE_COMMUNITY, nodeCommunity).iterator();
        if (!result.hasNext())
            throw new IllegalArgumentException("node community not found: " + nodeCommunity);

        Vertex vertex = result.next();
        int community = vertex.getProperty(COMMUNITY);
        return community;
    }

    @Override
    public int getCommunityFromNode(int nodeId)
    {
        Vertex vertex = graph.getVertices(NODE_ID, nodeId).iterator().next();
        return vertex.getProperty(COMMUNITY);
    }

    @Override
    public int getCommunitySize(int community)
    {
        Iterable<Vertex> vertices = graph.getVertices(COMMUNITY, community);
        Set<Integer> nodeCommunities = new HashSet<Integer>();
        for (Vertex v : vertices)
        {
            int nodeCommunity = v.getProperty(NODE_COMMUNITY);
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
            Iterator<Vertex> verticesIter = graph.getVertices(COMMUNITY, i).iterator();
            List<Integer> vertices = new ArrayList<Integer>();
            while (verticesIter.hasNext())
            {
                Integer nodeId = verticesIter.next().getProperty(NODE_ID);
                vertices.add(nodeId);
            }
            communities.put(i, vertices);
        }
        return communities;
    }

    protected void createSchema()
    {
        graph.executeOutsideTx(new OCallable<Object, OrientBaseGraph>() {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            @Override
            public Object call(final OrientBaseGraph g)
            {
                OrientVertexType v = g.getVertexBaseType();
                if(!v.existsProperty(NODE_ID)) { // TODO fix schema detection hack later
                    v.createProperty(NODE_ID, OType.INTEGER);
                    g.createKeyIndex(NODE_ID, Vertex.class, new Parameter("type", "UNIQUE_HASH_INDEX"), new Parameter(
                        "keytype", "INTEGER"));

                    v.createEdgeProperty(Direction.OUT, SIMILAR, OType.LINKBAG);
                    v.createEdgeProperty(Direction.IN, SIMILAR, OType.LINKBAG);
                    OrientEdgeType similar = g.createEdgeType(SIMILAR);
                    similar.createProperty("out", OType.LINK, v);
                    similar.createProperty("in", OType.LINK, v);
                    g.createKeyIndex(COMMUNITY, Vertex.class, new Parameter("type", "NOTUNIQUE_HASH_INDEX"),
                        new Parameter("keytype", "INTEGER"));
                    g.createKeyIndex(NODE_COMMUNITY, Vertex.class, new Parameter("type", "NOTUNIQUE_HASH_INDEX"),
                        new Parameter("keytype", "INTEGER"));
                }

                return null;
            }
        });
    }

    private OrientGraph getGraph(final File dbPath)
    {
        OrientGraph g;
        OrientGraphFactory graphFactory = new OrientGraphFactory("plocal:" + dbPath.getAbsolutePath());
        g = graphFactory.getTx();
        g.setUseLightweightEdges(this.useLightWeightEdges);
        return g;
    }

    @Override
    public boolean nodeExists(int nodeId)
    {
        Iterable<Vertex> iter = graph.getVertices(NODE_ID, nodeId);
        return iter.iterator().hasNext();
    }

    @Override
    public Iterator<Vertex> getVertexIterator()
    {
        return graph.getVertices().iterator();
    }

    @Override
    public Iterator<Edge> getNeighborsOfVertex(Vertex v)
    {
        return v.getEdges(Direction.BOTH, SIMILAR).iterator();
    }

    @Override
    public void cleanupVertexIterator(Iterator<Vertex> it)
    {
        // NOOP for timing
    }

    @Override
    public Vertex getOtherVertexFromEdge(Edge edge, Vertex oneVertex)
    {
        return edge.getVertex(Direction.IN).equals(oneVertex) ? edge.getVertex(Direction.OUT) : edge.getVertex(Direction.IN);
    }

    @Override
    public Iterator<Edge> getAllEdges()
    {
        return graph.getEdges().iterator();
    }

    @Override
    public Vertex getSrcVertexFromEdge(Edge edge)
    {
        return edge.getVertex(Direction.IN);
    }

    @Override
    public Vertex getDestVertexFromEdge(Edge edge)
    {
        return edge.getVertex(Direction.OUT);
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
        return graph.getVertices(NODE_ID, i).iterator().next();
    }
}
