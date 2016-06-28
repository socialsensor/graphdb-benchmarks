package eu.socialsensor.graphdatabases;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.sparsity.sparksee.algorithms.SinglePairShortestPathBFS;
import com.sparsity.sparksee.gdb.AttributeKind;
import com.sparsity.sparksee.gdb.Condition;
import com.sparsity.sparksee.gdb.DataType;
import com.sparsity.sparksee.gdb.Database;
import com.sparsity.sparksee.gdb.EdgeData;
import com.sparsity.sparksee.gdb.EdgesDirection;
import com.sparsity.sparksee.gdb.Graph;
import com.sparsity.sparksee.gdb.Objects;
import com.sparsity.sparksee.gdb.ObjectsIterator;
import com.sparsity.sparksee.gdb.Session;
import com.sparsity.sparksee.gdb.Sparksee;
import com.sparsity.sparksee.gdb.SparkseeConfig;
import com.sparsity.sparksee.gdb.Value;

import eu.socialsensor.insert.Insertion;
import eu.socialsensor.insert.SparkseeMassiveInsertion;
import eu.socialsensor.insert.SparkseeSingleInsertion;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.BenchmarkingException;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;

/**
 * Sparksee graph database implementation
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 */
public class SparkseeGraphDatabase extends GraphDatabaseBase<ObjectsIterator, ObjectsIterator, Long, Long>
{
    public static final String NODE = "node";

    public static final String INSERTION_TIMES_OUTPUT_PATH = "data/sparksee.insertion.times";

    private final String sparkseeLicenseKey;

    private boolean readOnly = false;

    double totalWeight;

    private SparkseeConfig sparkseeConfig;
    private Sparksee sparksee;
    private Database database;
    private Session session;
    private Graph sparkseeGraph;

    public static int NODE_ATTRIBUTE;
    public static int COMMUNITY_ATTRIBUTE;
    public static int NODE_COMMUNITY_ATTRIBUTE;

    public static int NODE_TYPE;

    public static int EDGE_TYPE;

    Value value = new Value();

    public SparkseeGraphDatabase(BenchmarkConfiguration config, File dbStorageDirectoryIn)
    {
        super(GraphDatabaseType.SPARKSEE, dbStorageDirectoryIn);
        this.sparkseeLicenseKey = config.getSparkseeLicenseKey();
    }

    @Override
    public void open()
    {
        sparkseeConfig = new SparkseeConfig();
        sparkseeConfig.setLicense(sparkseeLicenseKey);
        sparksee = new Sparksee(sparkseeConfig);
        try
        {
            this.database = sparksee.open(getDbFile(dbStorageDirectory), readOnly);
        }
        catch (FileNotFoundException e)
        {
            throw new BenchmarkingException("unable to open the db storage directory for sparksee", e);
        }
        this.session = database.newSession();
        this.sparkseeGraph = session.getGraph();
        createSchema();
    }

    private String getDbFile(File dbPath)
    {
        return new File(dbPath, "SparkseeDB.gdb").getAbsolutePath();
    }

    @Override
    public void createGraphForSingleLoad()
    {
        try
        {
            dbStorageDirectory.mkdirs();
            sparkseeConfig = new SparkseeConfig();
            sparkseeConfig.setLicense(sparkseeLicenseKey);
            sparksee = new Sparksee(sparkseeConfig);
            database = sparksee.create(getDbFile(dbStorageDirectory), "SparkseeDB");
            session = database.newSession();
            sparkseeGraph = session.getGraph();
            createSchema();
        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }

    }

    @Override
    public void createGraphForMassiveLoad()
    {
        // maybe some more configuration?
        try
        {
            dbStorageDirectory.mkdirs();
            sparkseeConfig = new SparkseeConfig();
            sparkseeConfig.setLicense(sparkseeLicenseKey);
            sparksee = new Sparksee(sparkseeConfig);
            database = sparksee.create(getDbFile(dbStorageDirectory), "SparkseeDB");
            session = database.newSession();
            sparkseeGraph = session.getGraph();
            createSchema();
        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }
    }

    private void createSchema()
    {
        NODE_TYPE = sparkseeGraph.newNodeType(NODE);
        NODE_ATTRIBUTE = sparkseeGraph.newAttribute(NODE_TYPE, NODE_ID, DataType.String, AttributeKind.Unique);
        EDGE_TYPE = sparkseeGraph.newEdgeType(SIMILAR, true, false);
        COMMUNITY_ATTRIBUTE = sparkseeGraph.newAttribute(NODE_TYPE, COMMUNITY, DataType.Integer,
            AttributeKind.Indexed);
        NODE_COMMUNITY_ATTRIBUTE = sparkseeGraph.newAttribute(NODE_TYPE, NODE_COMMUNITY, DataType.Integer,
            AttributeKind.Indexed);
    }

    @Override
    public void massiveModeLoading(File dataPath)
    {
        Insertion sparkseeMassiveInsertion = new SparkseeMassiveInsertion(session);
        sparkseeMassiveInsertion.createGraph(dataPath, 0 /* scenarioNumber */);
    }

    @Override
    public void singleModeLoading(File dataPath, File resultsPath, int scenarioNumber)
    {
        Insertion sparkseeSingleInsertion = new SparkseeSingleInsertion(this.session, resultsPath);
        sparkseeSingleInsertion.createGraph(dataPath, scenarioNumber);
    }

    @Override
    public void shutdown()
    {
        if (session != null)
        {
            session.close();
            session = null;
            database.close();
            database = null;
            sparksee.close();
            sparksee = null;
        }

    }

    @Override
    public void shutdownMassiveGraph()
    {
        shutdown();
    }

    @Override
    public void delete()
    {
        Utils.deleteRecursively(dbStorageDirectory);
    }

    @Override
    public void shortestPath(final Long srcNodeID, Integer i)
    {
        int nodeType = sparkseeGraph.findType(NODE);
        int edgeType = sparkseeGraph.findType(SIMILAR);

        long dstNodeID = getVertex(i);
        SinglePairShortestPathBFS shortestPathBFS = new SinglePairShortestPathBFS(session, srcNodeID, dstNodeID);
        shortestPathBFS.addNodeType(nodeType);
        shortestPathBFS.addEdgeType(edgeType, EdgesDirection.Outgoing);
        shortestPathBFS.setMaximumHops(4);
        shortestPathBFS.run();
        shortestPathBFS.close();
    }

    @Override
    public int getNodeCount()
    {
        return (int) sparkseeGraph.countNodes();
    }

    @Override
    public Set<Integer> getNeighborsIds(int nodeId)
    {
        Set<Integer> neighbors = new HashSet<Integer>();
        long nodeID = sparkseeGraph.findObject(NODE_ATTRIBUTE, value.setString(String.valueOf(nodeId)));
        Objects neighborsObjects = sparkseeGraph.neighbors(nodeID, EDGE_TYPE, EdgesDirection.Outgoing);
        ObjectsIterator neighborsIter = neighborsObjects.iterator();
        while (neighborsIter.hasNext())
        {
            long neighborID = neighborsIter.next();
            Value neighborNodeID = sparkseeGraph.getAttribute(neighborID, NODE_ATTRIBUTE);
            neighbors.add(Integer.valueOf(neighborNodeID.getString()));
        }
        neighborsIter.close();
        neighborsObjects.close();
        return neighbors;
    }

    @Override
    public double getNodeWeight(int nodeId)
    {
        long nodeID = sparkseeGraph.findObject(NODE_ATTRIBUTE, value.setString(String.valueOf(nodeId)));
        return getNodeOutDegree(nodeID);
    }

    public double getNodeInDegree(long node)
    {
        long inDegree = sparkseeGraph.degree(node, EDGE_TYPE, EdgesDirection.Ingoing);
        return (double) inDegree;
    }

    public double getNodeOutDegree(long node)
    {
        long outDegree = sparkseeGraph.degree(node, EDGE_TYPE, EdgesDirection.Outgoing);
        return (double) outDegree;
    }

    @Override
    public void initCommunityProperty()
    {
        int communityCounter = 0;
        // basic or indexed attribute?
        Objects nodes = sparkseeGraph.select(NODE_TYPE);
        ObjectsIterator nodesIter = nodes.iterator();
        while (nodesIter.hasNext())
        {
            long nodeID = nodesIter.next();
            sparkseeGraph.setAttribute(nodeID, COMMUNITY_ATTRIBUTE, value.setInteger(communityCounter));
            sparkseeGraph.setAttribute(nodeID, NODE_COMMUNITY_ATTRIBUTE, value.setInteger(communityCounter));
            communityCounter++;
        }
        nodesIter.close();
        nodes.close();
    }

    @Override
    public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities)
    {
        Set<Integer> communities = new HashSet<Integer>();
        Objects nodes = sparkseeGraph.select(NODE_COMMUNITY_ATTRIBUTE, Condition.Equal,
            value.setInteger(nodeCommunities));
        ObjectsIterator nodesIter = nodes.iterator();
        while (nodesIter.hasNext())
        {
            long nodeID = nodesIter.next();
            Objects neighbors = sparkseeGraph.neighbors(nodeID, EDGE_TYPE, EdgesDirection.Outgoing);
            ObjectsIterator neighborsIter = neighbors.iterator();
            while (neighborsIter.hasNext())
            {
                long neighborID = neighborsIter.next();
                Value community = sparkseeGraph.getAttribute(neighborID, COMMUNITY_ATTRIBUTE);
                communities.add(community.getInteger());
            }
            neighborsIter.close();
            neighbors.close();
        }
        nodesIter.close();
        nodes.close();
        return communities;
    }

    @Override
    public Set<Integer> getNodesFromCommunity(int community)
    {
        Set<Integer> nodesFromCommunity = new HashSet<Integer>();
        Objects nodes = sparkseeGraph.select(COMMUNITY_ATTRIBUTE, Condition.Equal, value.setInteger(community));
        ObjectsIterator nodesIter = nodes.iterator();
        while (nodesIter.hasNext())
        {
            Value nodeId = sparkseeGraph.getAttribute(nodesIter.next(), NODE_ATTRIBUTE);
            nodesFromCommunity.add(Integer.valueOf(nodeId.getString()));
        }
        nodesIter.close();
        nodes.close();
        return nodesFromCommunity;
    }

    @Override
    public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity)
    {
        Set<Integer> nodesFromNodeCommunity = new HashSet<Integer>();
        Objects nodes = sparkseeGraph
            .select(NODE_COMMUNITY_ATTRIBUTE, Condition.Equal, value.setInteger(nodeCommunity));
        ObjectsIterator nodesIter = nodes.iterator();
        while (nodesIter.hasNext())
        {
            Value nodeId = sparkseeGraph.getAttribute(nodesIter.next(), NODE_ATTRIBUTE);
            nodesFromNodeCommunity.add(Integer.valueOf(nodeId.getString()));
        }
        nodesIter.close();
        nodes.close();
        return nodesFromNodeCommunity;
    }

    @Override
    public double getEdgesInsideCommunity(int nodeCommunity, int communityNode)
    {
        double edges = 0;
        Objects nodesFromNodeCommunitiy = sparkseeGraph.select(NODE_COMMUNITY_ATTRIBUTE, Condition.Equal,
            value.setInteger(nodeCommunity));
        Objects nodesFromCommunity = sparkseeGraph.select(COMMUNITY_ATTRIBUTE, Condition.Equal,
            value.setInteger(communityNode));
        ObjectsIterator nodesFromNodeCommunityIter = nodesFromNodeCommunitiy.iterator();
        while (nodesFromNodeCommunityIter.hasNext())
        {
            long nodeID = nodesFromNodeCommunityIter.next();
            Objects neighbors = sparkseeGraph.neighbors(nodeID, EDGE_TYPE, EdgesDirection.Outgoing);
            ObjectsIterator neighborsIter = neighbors.iterator();
            while (neighborsIter.hasNext())
            {
                if (nodesFromCommunity.contains(neighborsIter.next()))
                {
                    edges++;
                }
            }
            neighborsIter.close();
            neighbors.close();
        }
        nodesFromNodeCommunityIter.close();
        nodesFromCommunity.close();
        nodesFromNodeCommunitiy.close();
        return edges;
    }

    @Override
    public double getCommunityWeight(int community)
    {
        double communityWeight = 0;
        Objects nodesFromCommunity = sparkseeGraph.select(COMMUNITY_ATTRIBUTE, Condition.Equal,
            value.setInteger(community));
        ObjectsIterator nodesFromCommunityIter = nodesFromCommunity.iterator();
        if (nodesFromCommunity.size() > 1)
        {
            while (nodesFromCommunityIter.hasNext())
            {
                communityWeight += getNodeOutDegree(nodesFromCommunityIter.next());
            }
        }
        nodesFromCommunityIter.close();
        nodesFromCommunity.close();
        return communityWeight;
    }

    @Override
    public double getNodeCommunityWeight(int nodeCommunity)
    {
        double nodeCommunityWeight = 0;
        Objects nodesFromNodeCommunity = sparkseeGraph.select(NODE_COMMUNITY_ATTRIBUTE, Condition.Equal,
            value.setInteger(nodeCommunity));
        ObjectsIterator nodesFromNodeCommunityIter = nodesFromNodeCommunity.iterator();
        if (nodesFromNodeCommunity.size() > 1)
        {
            while (nodesFromNodeCommunityIter.hasNext())
            {
                nodeCommunityWeight += getNodeOutDegree(nodesFromNodeCommunityIter.next());
            }
        }
        nodesFromNodeCommunityIter.close();
        nodesFromNodeCommunity.close();
        return nodeCommunityWeight;
    }

    @Override
    public void moveNode(int nodeCommunity, int toCommunity)
    {
        Objects fromNodes = sparkseeGraph.select(NODE_COMMUNITY_ATTRIBUTE, Condition.Equal,
            value.setInteger(nodeCommunity));
        ObjectsIterator fromNodesIter = fromNodes.iterator();
        while (fromNodesIter.hasNext())
        {
            sparkseeGraph.setAttribute(fromNodesIter.next(), COMMUNITY_ATTRIBUTE, value.setInteger(toCommunity));
        }
        fromNodesIter.close();
        fromNodes.close();
    }

    @Override
    public double getGraphWeightSum()
    {
        return (double) sparkseeGraph.countEdges();
    }

    @Override
    public int reInitializeCommunities()
    {
        Map<Integer, Integer> initCommunities = new HashMap<Integer, Integer>();
        int communityCounter = 0;
        Objects nodes = sparkseeGraph.select(NODE_TYPE);
        ObjectsIterator nodesIter = nodes.iterator();
        while (nodesIter.hasNext())
        {
            long nodeID = nodesIter.next();
            Value communityId = sparkseeGraph.getAttribute(nodeID, COMMUNITY_ATTRIBUTE);
            if (!initCommunities.containsKey(communityId.getInteger()))
            {
                initCommunities.put(communityId.getInteger(), communityCounter);
                communityCounter++;
            }
            int newCommunityId = initCommunities.get(communityId.getInteger());
            sparkseeGraph.setAttribute(nodeID, COMMUNITY_ATTRIBUTE, value.setInteger(newCommunityId));
            sparkseeGraph.setAttribute(nodeID, NODE_COMMUNITY_ATTRIBUTE, value.setInteger(newCommunityId));
        }
        nodesIter.close();
        nodes.close();
        return communityCounter;
    }

    @Override
    public int getCommunity(int nodeCommunity)
    {
        long nodeID = sparkseeGraph.findObject(NODE_COMMUNITY_ATTRIBUTE, value.setInteger(nodeCommunity));
        Value communityId = sparkseeGraph.getAttribute(nodeID, COMMUNITY_ATTRIBUTE);
        return communityId.getInteger();
    }

    @Override
    public int getCommunityFromNode(int nodeId)
    {
        long nodeID = sparkseeGraph.findObject(NODE_ATTRIBUTE, value.setString(String.valueOf(nodeId)));
        Value communityId = sparkseeGraph.getAttribute(nodeID, COMMUNITY_ATTRIBUTE);
        return communityId.getInteger();
    }

    @Override
    public int getCommunitySize(int community)
    {
        Objects nodesFromCommunities = sparkseeGraph.select(COMMUNITY_ATTRIBUTE, Condition.Equal,
            value.setInteger(community));
        ObjectsIterator nodesFromCommunitiesIter = nodesFromCommunities.iterator();
        Set<Integer> nodeCommunities = new HashSet<Integer>();
        while (nodesFromCommunitiesIter.hasNext())
        {
            Value nodeCommunityId = sparkseeGraph.getAttribute(nodesFromCommunitiesIter.next(),
                NODE_COMMUNITY_ATTRIBUTE);
            nodeCommunities.add(nodeCommunityId.getInteger());
        }
        nodesFromCommunitiesIter.close();
        nodesFromCommunities.close();
        return nodeCommunities.size();
    }

    @Override
    public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities)
    {
        Map<Integer, List<Integer>> communities = new HashMap<Integer, List<Integer>>();
        for (int i = 0; i < numberOfCommunities; i++)
        {
            Objects nodesFromCommunity = sparkseeGraph
                .select(COMMUNITY_ATTRIBUTE, Condition.Equal, value.setInteger(i));
            ObjectsIterator nodesFromCommunityIter = nodesFromCommunity.iterator();
            List<Integer> nodes = new ArrayList<Integer>();
            while (nodesFromCommunityIter.hasNext())
            {
                Value nodeId = sparkseeGraph.getAttribute(nodesFromCommunityIter.next(), NODE_ATTRIBUTE);
                nodes.add(Integer.valueOf(nodeId.getString()));
            }
            communities.put(i, nodes);
            nodesFromCommunityIter.close();
            nodesFromCommunity.close();
        }
        return communities;
    }

    @Override
    public boolean nodeExists(int nodeId)
    {
        Objects nodes = sparkseeGraph.select(NODE_ATTRIBUTE, Condition.Equal, value.setInteger(nodeId));
        ObjectsIterator nodesIter = nodes.iterator();
        if (nodesIter.hasNext())
        {
            nodesIter.close();
            nodes.close();
            return true;
        }
        nodesIter.close();
        nodes.close();
        return false;
    }

    @Override
    public ObjectsIterator getVertexIterator()
    {
        final int nodeType = sparkseeGraph.findType(NODE);
        final Objects objects = sparkseeGraph.select(nodeType);
        return objects.iterator();
    }

    @Override
    public ObjectsIterator getNeighborsOfVertex(Long v)
    {
        final int edgeType = sparkseeGraph.findType(SIMILAR);
        final Objects neighbors = sparkseeGraph.neighbors(v, edgeType, EdgesDirection.Any);
        return neighbors.iterator();
    }

    @Override
    public void cleanupVertexIterator(ObjectsIterator it)
    {
        it.close();
    }

    @Override
    public Long getOtherVertexFromEdge(Long r, Long oneVertex)
    {
        return r; //pass through
    }

    @Override
    public ObjectsIterator getAllEdges()
    {
        int edgeType = sparkseeGraph.findType(SIMILAR);
        Objects objects = sparkseeGraph.select(edgeType);
        return objects.iterator();
    }

    @Override
    public Long getSrcVertexFromEdge(Long edge)
    {
        EdgeData edgeData = sparkseeGraph.getEdgeData(edge);
        return edgeData.getTail();
    }

    @Override
    public Long getDestVertexFromEdge(Long edge)
    {
        EdgeData edgeData = sparkseeGraph.getEdgeData(edge);
        return edgeData.getHead();
    }

    @Override
    public boolean edgeIteratorHasNext(ObjectsIterator it)
    {
        return it.hasNext();
    }

    @Override
    public Long nextEdge(ObjectsIterator it)
    {
        return it.next();
    }

    @Override
    public void cleanupEdgeIterator(ObjectsIterator it)
    {
        it.close();
    }

    @Override
    public boolean vertexIteratorHasNext(ObjectsIterator it)
    {
        return it.hasNext();
    }

    @Override
    public Long nextVertex(ObjectsIterator it)
    {
        return it.next();
    }

    @Override
    public Long getVertex(Integer i)
    {
        int nodeType = sparkseeGraph.findType(NODE);
        int nodeAttribute = sparkseeGraph.findAttribute(nodeType, NODE_ID);
        return sparkseeGraph.findObject(nodeAttribute, value.setInteger(i));
    }
}
