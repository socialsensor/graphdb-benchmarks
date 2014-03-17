package eu.socialsensor.graphdatabases;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tinkerpop.blueprints.Vertex;

public interface GraphDatabase {
	
	public void open(String dbPath);
	
	public void createGraphForSingleLoad(String dbPath);
	
	public void massiveModeLoading(String dataPath);
	
	public void singleModeLoading(String dataPath);
	
	public void createGraphForMassiveLoad(String dbPath);
		
	public void shutdown();
	
	public void shutdownMassiveGraph();
	
	public void shorestPathQuery();
	
	public void neighborsOfAllNodesQuery();
	
	public void nodesOfAllEdgesQuery();
		
	public int getNodeCount();
		
	public Set<Integer> getNeighborsIds(int nodeId);
		
	public double getNodeWeight(int nodeId);
		
	public void initCommunityProperty();
	
	public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities);
	
	public Set<Integer> getNodesFromCommunity(int community);
	
	public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity);
	
	public double getEdgesInsideCommunity(int nodeCommunity, int communityNodes);
	
	public double getCommunityWeight(int community);
	
	public double getNodeCommunityWeight(int nodeCommunity);
	
	public void moveNode(int from, int to);
	
	public double getGraphWeightSum();
	
	public void testCommunities();
	
	public int reInitializeCommunities2();
	
	public void printCommunities();
	
	public int getCommunityFromNode(int nodeId);
	
	public int getCommunity(int nodeCommunity);
	
	public int getCommunitySize(int community);
	
	public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities);
	
}
