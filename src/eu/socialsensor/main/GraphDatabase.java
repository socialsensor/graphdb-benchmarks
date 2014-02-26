package eu.socialsensor.main;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.tinkerpop.blueprints.Vertex;

public interface GraphDatabase {
	
	public void open(String dbPath);
	
	public void createGraphForSingleLoad(String dbPath);
	
	public void createGraphForMassiveLoad(String dbPath);
	
	public void shutdown();
	
	public void shutdownMassiveGraph();
		
	public int getNodeCount();
	
	public Iterable getNodes();
	
	public List<Long> getNodeIds();
	
	public List<Long> getNeighborsIds(long nodeId);
	
	public double getNodeDegree(long nodeId);
	
	public double getNodeInDegree(Vertex vertex);
	
	public double getNodeOutDegree(Vertex vertex);
	
	public void initCommunityProperty();
	
	public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities);
	
	public LinkedList<Vertex> getNodesFromCommunity(int community);
	
	public double getEdgesInsideCommunity(int nodeCommunity, int communityNodes);
	
	public double getCommunityWeight(int community);
	
	public void moveNode(int from, int to);
	
	public int getNumberOfCommunities();
	
	public double getGraphWeightSum();
}
