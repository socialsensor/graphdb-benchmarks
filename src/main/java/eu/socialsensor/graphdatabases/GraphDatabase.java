package eu.socialsensor.graphdatabases;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Represents a graph database
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 */
public interface GraphDatabase<VertexIteratorType, EdgeIteratorType, VertexType, EdgeType>
{
    //edge and vertex operations
    public VertexType getOtherVertexFromEdge(EdgeType r, VertexType oneVertex);
    public VertexType getSrcVertexFromEdge(EdgeType edge);
    public VertexType getDestVertexFromEdge(EdgeType edge);
    public VertexType getVertex(Integer i);
    
    //edge iterators
    public EdgeIteratorType getAllEdges();
    public EdgeIteratorType getNeighborsOfVertex(VertexType v);
    public boolean edgeIteratorHasNext(EdgeIteratorType it);
    public EdgeType nextEdge(EdgeIteratorType it);
    public void cleanupEdgeIterator(EdgeIteratorType it);
    
    //vertex iterators
    public VertexIteratorType getVertexIterator();
    public boolean vertexIteratorHasNext(VertexIteratorType it);
    public VertexType nextVertex(VertexIteratorType it);
    public void cleanupVertexIterator(VertexIteratorType it);
    
    //benchmarks
    public void findAllNodeNeighbours();
    public void findNodesOfAllEdges();

    /**
     * Opens the graph database
     * 
     * @param dbPath
     *            - database path
     */
    public void open();

    /**
     * Creates a graph database and configures for single data insertion
     * 
     * @param dbPath
     *            - database path
     */
    public void createGraphForSingleLoad();

    /**
     * Inserts data in massive mode
     * 
     * @param dataPath
     *            - dataset path
     */
    public void massiveModeLoading(File dataPath);

    /**
     * Inserts data in single mode
     * 
     * @param dataPath
     *            - dataset path
     * @param resultsPath
     * @param scenarioNumber
     */
    public void singleModeLoading(File dataPath, File resultsPath, int scenarioNumber);

    /**
     * Creates a graph database and configures for bulk data insertion
     * 
     * @param dataPath
     *            - dataset path
     */
    public void createGraphForMassiveLoad();

    /**
     * Shut down the graph database
     */
    public void shutdown();

    /**
     * Delete the graph database
     */
    public void delete();

    /**
     * Shutdown the graph database, which configuration is for massive insertion
     * of data
     */
    public void shutdownMassiveGraph();

    /**
     * Find the shortest path between vertex 1 and each of the vertexes in the list
     * 
     * @param nodes
     *            any number of random nodes
     */
    public void shortestPaths(Set<Integer> nodes);
    
    /**
     * Execute findShortestPaths query from the Query interface
     * 
     * @param nodes
     *            any number of random nodes
     */
    public void shortestPath(final VertexType fromNode, Integer node);

    /**
     * @return the number of nodes
     */
    public int getNodeCount();

    /**
     * @param nodeId
     * @return the neighbours of a particular node
     */
    public Set<Integer> getNeighborsIds(int nodeId);

    /**
     * @param nodeId
     * @return the node degree
     */
    public double getNodeWeight(int nodeId);

    /**
     * Initializes the community and nodeCommunity property in each database
     */
    public void initCommunityProperty();

    /**
     * @param nodeCommunities
     * @return the communities (communityId) that are connected with a
     *         particular nodeCommunity
     */
    public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities);

    /**
     * @param community
     * @return the nodes a particular community contains
     */
    public Set<Integer> getNodesFromCommunity(int community);

    /**
     * @param nodeCommunity
     * @return the nodes a particular nodeCommunity contains
     */
    public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity);

    /**
     * @param nodeCommunity
     * @param communityNodes
     * @return the number of edges between a community and a nodeCommunity
     */
    public double getEdgesInsideCommunity(int nodeCommunity, int communityNodes);

    /**
     * @param community
     * @return the sum of node degrees
     */
    public double getCommunityWeight(int community);

    /**
     * @param nodeCommunity
     * @return the sum of node degrees
     */
    public double getNodeCommunityWeight(int nodeCommunity);

    /**
     * Moves a node from a community to another
     * 
     * @param from
     * @param to
     */
    public void moveNode(int from, int to);

    /**
     * @return the number of edges of the graph database
     */
    public double getGraphWeightSum();

    /**
     * Reinitializes the community and nodeCommunity property
     * 
     * @return the number of communities
     */
    public int reInitializeCommunities();

    /**
     * @param nodeId
     * @return in which community a particular node belongs
     */
    public int getCommunityFromNode(int nodeId);

    /**
     * @param nodeCommunity
     * @return in which community a particular nodeCommunity belongs
     */
    public int getCommunity(int nodeCommunity);

    /**
     * @param community
     * @return the number of nodeCommunities a particular community contains
     */
    public int getCommunitySize(int community);

    /**
     * @param numberOfCommunities
     * @return a map where the key is the community id and the value is the
     *         nodes each community has.
     */
    public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities);

    /**
     * 
     * @param nodeId
     * @return return true if node exist, false if not
     */
    public boolean nodeExists(int nodeId);
}
