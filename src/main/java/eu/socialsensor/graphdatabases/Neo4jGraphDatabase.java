package eu.socialsensor.graphdatabases;

import com.google.common.collect.Iterables;

import eu.socialsensor.insert.Insertion;
import eu.socialsensor.insert.Neo4jMassiveInsertion;
import eu.socialsensor.insert.Neo4jSingleInsertion;
import eu.socialsensor.main.BenchmarkingException;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.TransactionBuilder;
import org.neo4j.kernel.Traversal;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Neo4j graph database implementation
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 */
public class Neo4jGraphDatabase extends GraphDatabaseBase<Iterator<Node>, Iterator<Relationship>, Node, Relationship>
{
    protected GraphDatabaseService neo4jGraph = null;
    private Schema schema = null;

    private BatchInserter inserter = null;

    public static enum RelTypes implements RelationshipType
    {
        SIMILAR
    }

    public static Label NODE_LABEL = DynamicLabel.label("Node");

    public Neo4jGraphDatabase(File dbStorageDirectoryIn)
    {
        super(GraphDatabaseType.NEO4J, dbStorageDirectoryIn);
    }

    @Override
    public void open()
    {
        neo4jGraph = new GraphDatabaseFactory().newEmbeddedDatabase(dbStorageDirectory.getAbsolutePath());
        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                neo4jGraph.schema().awaitIndexesOnline(10l, TimeUnit.MINUTES);
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unknown error", e);
            }
        }
    }

    @Override
    public void createGraphForSingleLoad()
    {
        neo4jGraph = new GraphDatabaseFactory().newEmbeddedDatabase(dbStorageDirectory.getAbsolutePath());
        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                schema = neo4jGraph.schema();
                schema.indexFor(NODE_LABEL).on(NODE_ID).create();
                schema.indexFor(NODE_LABEL).on(COMMUNITY).create();
                schema.indexFor(NODE_LABEL).on(NODE_COMMUNITY).create();
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unknown error", e);
            }
        }
    }

    @Override
    public void createGraphForMassiveLoad()
    {
        Map<String, String> config = new HashMap<String, String>();
        config.put("cache_type", "none");
        config.put("use_memory_mapped_buffers", "true");
        config.put("neostore.nodestore.db.mapped_memory", "200M");
        config.put("neostore.relationshipstore.db.mapped_memory", "1000M");
        config.put("neostore.propertystore.db.mapped_memory", "250M");
        config.put("neostore.propertystore.db.strings.mapped_memory", "250M");

        inserter = BatchInserters.inserter(dbStorageDirectory.getAbsolutePath(), config);
        createDeferredSchema();
    }

    private void createDeferredSchema()
    {
        inserter.createDeferredSchemaIndex(NODE_LABEL).on(NODE_ID).create();
        inserter.createDeferredSchemaIndex(NODE_LABEL).on(COMMUNITY).create();
        inserter.createDeferredSchemaIndex(NODE_LABEL).on(NODE_COMMUNITY).create();
    }

    @Override
    public void singleModeLoading(File dataPath, File resultsPath, int scenarioNumber)
    {
        Insertion neo4jSingleInsertion = new Neo4jSingleInsertion(this.neo4jGraph, resultsPath);
        neo4jSingleInsertion.createGraph(dataPath, scenarioNumber);
    }

    @Override
    public void massiveModeLoading(File dataPath)
    {
        Insertion neo4jMassiveInsertion = new Neo4jMassiveInsertion(this.inserter);
        neo4jMassiveInsertion.createGraph(dataPath, 0 /* scenarioNumber */);
    }

    @Override
    public void shutdown()
    {
        if (neo4jGraph == null)
        {
            return;
        }
        neo4jGraph.shutdown();
    }

    @Override
    public void delete()
    {
        Utils.deleteRecursively(dbStorageDirectory);
    }

    @Override
    public void shutdownMassiveGraph()
    {
        if (inserter == null)
        {
            return;
        }
        inserter.shutdown();

        File store_lock = new File("graphDBs/Neo4j", "store_lock");
        store_lock.delete();
        if (store_lock.exists())
        {
            throw new BenchmarkingException("could not remove store_lock");
        }

        File lock = new File("graphDBs/Neo4j", "lock");
        lock.delete();
        if (lock.exists())
        {
            throw new BenchmarkingException("could not remove lock");
        }

        inserter = null;
    }

    @Override
    public void shortestPath(Node n1, Integer i)
    {
        PathFinder<Path> finder
            = GraphAlgoFactory.shortestPath(Traversal.expanderForTypes(Neo4jGraphDatabase.RelTypes.SIMILAR), 5);
        Node n2 = getVertex(i);
        Path path = finder.findSinglePath(n1, n2);

    }

    //TODO can unforced option be pulled into configuration?
    private Transaction beginUnforcedTransaction() {
        final TransactionBuilder builder = ((GraphDatabaseAPI) neo4jGraph).tx().unforced();
        return builder.begin();
    }

    @Override
    public int getNodeCount()
    {
        int nodeCount = 0;
        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                nodeCount = IteratorUtil.count(GlobalGraphOperations.at(neo4jGraph).getAllNodes());
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to get node count", e);
            }
        }

        return nodeCount;
    }

    @Override
    public Set<Integer> getNeighborsIds(int nodeId)
    {
        Set<Integer> neighbors = new HashSet<Integer>();
        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                Node n = neo4jGraph.findNodesByLabelAndProperty(NODE_LABEL, NODE_ID, String.valueOf(nodeId)).iterator()
                    .next();
                for (Relationship relationship : n.getRelationships(RelTypes.SIMILAR, Direction.OUTGOING))
                {
                    Node neighbour = relationship.getOtherNode(n);
                    String neighbourId = (String) neighbour.getProperty(NODE_ID);
                    neighbors.add(Integer.valueOf(neighbourId));
                }
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to get neighbors ids", e);
            }
        }

        return neighbors;
    }

    @Override
    public double getNodeWeight(int nodeId)
    {
        double weight = 0;
        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                Node n = neo4jGraph.findNodesByLabelAndProperty(NODE_LABEL, NODE_ID, String.valueOf(nodeId)).iterator()
                    .next();
                weight = getNodeOutDegree(n);
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to get node weight", e);
            }
        }

        return weight;
    }

    public double getNodeInDegree(Node node)
    {
        Iterable<Relationship> rel = node.getRelationships(Direction.OUTGOING, RelTypes.SIMILAR);
        return (double) (IteratorUtil.count(rel));
    }

    public double getNodeOutDegree(Node node)
    {
        Iterable<Relationship> rel = node.getRelationships(Direction.INCOMING, RelTypes.SIMILAR);
        return (double) (IteratorUtil.count(rel));
    }

    @Override
    public void initCommunityProperty()
    {
        int communityCounter = 0;

        // maybe commit changes every 1000 transactions?
        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                for (Node n : GlobalGraphOperations.at(neo4jGraph).getAllNodes())
                {
                    n.setProperty(NODE_COMMUNITY, communityCounter);
                    n.setProperty(COMMUNITY, communityCounter);
                    communityCounter++;
                }
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to initialize community property", e);
            }
        }
    }

    @Override
    public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities)
    {
        Set<Integer> communities = new HashSet<Integer>();
        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                ResourceIterable<Node> nodes = neo4jGraph.findNodesByLabelAndProperty(Neo4jGraphDatabase.NODE_LABEL,
                    NODE_COMMUNITY, nodeCommunities);
                for (Node n : nodes)
                {
                    for (Relationship r : n.getRelationships(RelTypes.SIMILAR, Direction.OUTGOING))
                    {
                        Node neighbour = r.getOtherNode(n);
                        Integer community = (Integer) (neighbour.getProperty(COMMUNITY));
                        communities.add(community);
                    }
                }
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to get communities connected to node communities", e);
            }
        }

        return communities;
    }

    @Override
    public Set<Integer> getNodesFromCommunity(int community)
    {
        Set<Integer> nodes = new HashSet<Integer>();
        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                ResourceIterable<Node> iter = neo4jGraph.findNodesByLabelAndProperty(NODE_LABEL, COMMUNITY, community);
                for (Node n : iter)
                {
                    String nodeIdString = (String) (n.getProperty(NODE_ID));
                    nodes.add(Integer.valueOf(nodeIdString));
                }
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to get nodes from community", e);
            }
        }
        return nodes;
    }

    @Override
    public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity)
    {
        Set<Integer> nodes = new HashSet<Integer>();

        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                ResourceIterable<Node> iter = neo4jGraph.findNodesByLabelAndProperty(NODE_LABEL, NODE_COMMUNITY,
                    nodeCommunity);
                for (Node n : iter)
                {
                    String nodeIdString = (String) (n.getProperty(NODE_ID));
                    nodes.add(Integer.valueOf(nodeIdString));
                }
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to get nodes from node community", e);
            }
        }

        return nodes;
    }

    @Override
    public double getEdgesInsideCommunity(int nodeCommunity, int communityNodes)
    {
        double edges = 0;
        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                ResourceIterable<Node> nodes = neo4jGraph.findNodesByLabelAndProperty(NODE_LABEL, NODE_COMMUNITY,
                    nodeCommunity);
                ResourceIterable<Node> comNodes = neo4jGraph.findNodesByLabelAndProperty(NODE_LABEL, COMMUNITY,
                    communityNodes);
                for (Node node : nodes)
                {
                    Iterable<Relationship> relationships = node.getRelationships(RelTypes.SIMILAR, Direction.OUTGOING);
                    for (Relationship r : relationships)
                    {
                        Node neighbor = r.getOtherNode(node);
                        if (Iterables.contains(comNodes, neighbor))
                        {
                            edges++;
                        }
                    }
                }
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to get edges inside community", e);
            }
        }

        return edges;
    }

    @Override
    public double getCommunityWeight(int community)
    {
        double communityWeight = 0;
        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                ResourceIterable<Node> iter = neo4jGraph.findNodesByLabelAndProperty(NODE_LABEL, COMMUNITY, community);
                if (Iterables.size(iter) > 1)
                {
                    for (Node n : iter)
                    {
                        communityWeight += getNodeOutDegree(n);
                    }
                }
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to get community weight", e);
            }
        }

        return communityWeight;
    }

    @Override
    public double getNodeCommunityWeight(int nodeCommunity)
    {
        double nodeCommunityWeight = 0;
        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                ResourceIterable<Node> iter = neo4jGraph.findNodesByLabelAndProperty(NODE_LABEL, NODE_COMMUNITY,
                    nodeCommunity);
                if (Iterables.size(iter) > 1)
                {
                    for (Node n : iter)
                    {
                        nodeCommunityWeight += getNodeOutDegree(n);
                    }
                }
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to get node community weight", e);
            }
        }

        return nodeCommunityWeight;
    }

    @Override
    public void moveNode(int nodeCommunity, int toCommunity)
    {
        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                ResourceIterable<Node> fromIter = neo4jGraph.findNodesByLabelAndProperty(NODE_LABEL, NODE_COMMUNITY,
                    nodeCommunity);
                for (Node node : fromIter)
                {
                    node.setProperty(COMMUNITY, toCommunity);
                }
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to move node", e);
            }
        }
    }

    @Override
    public double getGraphWeightSum()
    {
        int edgeCount = 0;

        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                edgeCount = IteratorUtil.count(GlobalGraphOperations.at(neo4jGraph).getAllRelationships());
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to get graph weight sum", e);
            }
        }

        return (double) edgeCount;
    }

    @Override
    public int reInitializeCommunities()
    {
        Map<Integer, Integer> initCommunities = new HashMap<Integer, Integer>();
        int communityCounter = 0;

        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                for (Node n : GlobalGraphOperations.at(neo4jGraph).getAllNodes())
                {
                    Integer communityId = (Integer) (n.getProperty(COMMUNITY));
                    if (!initCommunities.containsKey(communityId))
                    {
                        initCommunities.put(communityId, communityCounter);
                        communityCounter++;
                    }
                    int newCommunityId = initCommunities.get(communityId);
                    n.setProperty(COMMUNITY, newCommunityId);
                    n.setProperty(NODE_COMMUNITY, newCommunityId);
                }
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to reinitialize communities", e);
            }
        }

        return communityCounter;
    }

    @Override
    public int getCommunity(int nodeCommunity)
    {
        Integer community = 0;

        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                Node node = neo4jGraph.findNodesByLabelAndProperty(NODE_LABEL, NODE_COMMUNITY, nodeCommunity).iterator()
                    .next();
                community = (Integer) (node.getProperty(COMMUNITY));
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to get community", e);
            }
        }

        return community;
    }

    @Override
    public int getCommunityFromNode(int nodeId)
    {
        Integer community = 0;
        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                // Node node = nodeIndex.get(NODE_ID, nodeId).getSingle();
                Node node = neo4jGraph.findNodesByLabelAndProperty(NODE_LABEL, NODE_ID, String.valueOf(nodeId)).iterator()
                    .next();
                community = (Integer) (node.getProperty(COMMUNITY));
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to get community from node", e);
            }
        }

        return community;
    }

    @Override
    public int getCommunitySize(int community)
    {
        Set<Integer> nodeCommunities = new HashSet<Integer>();

        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                ResourceIterable<Node> nodes = neo4jGraph.findNodesByLabelAndProperty(NODE_LABEL, COMMUNITY, community);
                for (Node n : nodes)
                {
                    Integer nodeCommunity = (Integer) (n.getProperty(COMMUNITY));
                    nodeCommunities.add(nodeCommunity);
                }
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to get community size", e);
            }
        }

        return nodeCommunities.size();
    }

    @Override
    public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities)
    {
        Map<Integer, List<Integer>> communities = new HashMap<Integer, List<Integer>>();

        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                for (int i = 0; i < numberOfCommunities; i++)
                {
                    ResourceIterable<Node> nodesIter = neo4jGraph.findNodesByLabelAndProperty(NODE_LABEL, COMMUNITY, i);
                    List<Integer> nodes = new ArrayList<Integer>();
                    for (Node n : nodesIter)
                    {
                        String nodeIdString = (String) (n.getProperty(NODE_ID));
                        nodes.add(Integer.valueOf(nodeIdString));
                    }
                    communities.put(i, nodes);
                }
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to map communities", e);
            }
        }

        return communities;
    }

    @Override
    public boolean nodeExists(int nodeId)
    {
        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                ResourceIterable<Node> nodesIter = neo4jGraph.findNodesByLabelAndProperty(NODE_LABEL, NODE_ID, nodeId);
                if (nodesIter.iterator().hasNext())
                {
                    tx.success();
                    return true;
                }
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to determine if node exists", e);
            }
        }
        return false;
    }

    @Override
    public Iterator<Node> getVertexIterator()
    {
        return GlobalGraphOperations.at(neo4jGraph).getAllNodes().iterator();
    }

    @Override
    public Iterator<Relationship> getNeighborsOfVertex(Node v)
    {
        return v.getRelationships(Neo4jGraphDatabase.RelTypes.SIMILAR, Direction.BOTH).iterator();
    }

    @Override
    public void cleanupVertexIterator(Iterator<Node> it)
    {
        // NOOP
    }

    @Override
    public Node getOtherVertexFromEdge(Relationship r, Node n)
    {
        return r.getOtherNode(n);
    }

    @Override
    public Iterator<Relationship> getAllEdges()
    {
        return GlobalGraphOperations.at(neo4jGraph).getAllRelationships().iterator();
    }

    @Override
    public Node getSrcVertexFromEdge(Relationship edge)
    {
        return edge.getStartNode();
    }

    @Override
    public Node getDestVertexFromEdge(Relationship edge)
    {
        return edge.getEndNode();
    }

    @Override
    public boolean edgeIteratorHasNext(Iterator<Relationship> it)
    {
        return it.hasNext();
    }

    @Override
    public Relationship nextEdge(Iterator<Relationship> it)
    {
        return it.next();
    }

    @Override
    public void cleanupEdgeIterator(Iterator<Relationship> it)
    {
        //NOOP
    }

    @Override
    public boolean vertexIteratorHasNext(Iterator<Node> it)
    {
        return it.hasNext();
    }

    @Override
    public Node nextVertex(Iterator<Node> it)
    {
        return it.next();
    }

    @Override
    public Node getVertex(Integer i)
    {
        // note, this probably should be run in the context of an active transaction.
        return neo4jGraph.findNodesByLabelAndProperty(Neo4jGraphDatabase.NODE_LABEL, NODE_ID, i).iterator()
            .next();
    }

}
