package eu.socialsensor.clustering;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import eu.socialsensor.main.GraphDatabase;

public class Cache {
	
	LoadingCache<Integer, Set<Integer>> nodeCommunitiesMap; //key=nodeCommunity value=nodeIds contained in nodeCommunityC
	LoadingCache<Integer, Set<Integer>> communitiesMap; //key=community value=nodeIds contained  in community
	LoadingCache<Integer, Integer> nodeCommunitiesToCommunities; //key=nodeCommunity value=community
	LoadingCache<Integer, Set<Integer>> nodeNeighbours; //key=nodeId value=nodeId neighbors
	LoadingCache<Integer, Double> nodeWeights; //key=nodeId value=weight
	LoadingCache<Integer, Integer> nodeToCommunityMap; //key=nodeId value=communityId
	LoadingCache<Integer, Integer> communitySize; //number of nodeCommunities per community
		
	public Cache(final GraphDatabase graphDatabase, int cacheSize) throws ExecutionException {
		
		nodeNeighbours = CacheBuilder.newBuilder()
				.maximumSize(cacheSize)
				.build(
						new CacheLoader<Integer, Set<Integer>>() {
							public Set<Integer> load(Integer nodeId) {
								return graphDatabase.getNeighborsIds(nodeId);
							}
						});
		
		nodeWeights = CacheBuilder.newBuilder()
				.maximumSize(cacheSize)
				.build(
						new CacheLoader<Integer, Double>() {
							public Double load(Integer nodeId) {
								return graphDatabase.getNodeWeight(nodeId);
							}
						});
			
					
		nodeNeighbours = CacheBuilder.newBuilder()
				.maximumSize(cacheSize)
				.build(
						new CacheLoader<Integer, Set<Integer>>() {
							public Set<Integer> load(Integer nodeId) {
								return graphDatabase.getNeighborsIds(nodeId);
							}
						});
		
		nodeCommunitiesMap = CacheBuilder.newBuilder()
				.maximumSize(cacheSize/4)
				.build(
						new CacheLoader<Integer, Set<Integer>>() {
							public Set<Integer> load(Integer nodeCommunityId) {
								return graphDatabase.getNodesFromNodeCommunity(nodeCommunityId);
							}
						});
		
		communitiesMap = CacheBuilder.newBuilder()
				.maximumSize(cacheSize)
				.build(
						new CacheLoader<Integer, Set<Integer>>() {
							public Set<Integer> load(Integer communityId) {
								return graphDatabase.getNodesFromCommunity(communityId);
							}
						});
		
		nodeToCommunityMap = CacheBuilder.newBuilder()
				.maximumSize(cacheSize)
				.build(
						new CacheLoader<Integer, Integer>() {
							public Integer load(Integer nodeId) {
								return graphDatabase.getCommunityFromNode(nodeId);
							}
						});
		
		communitySize = CacheBuilder.newBuilder()
				.maximumSize(cacheSize)
				.build(
						new CacheLoader<Integer, Integer>() {
							public Integer load(Integer communityId) {
								return graphDatabase.getCommunitySize(communityId);
							}
						});
				
		nodeCommunitiesToCommunities = CacheBuilder.newBuilder()
				.maximumSize(cacheSize)
				.build(
						new CacheLoader<Integer, Integer>() {
							public Integer load(Integer nodeCommunity) {
								return graphDatabase.getCommunity(nodeCommunity);
							}
						});
	}
	
	public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunity) throws ExecutionException {
		Set<Integer> nodesFromNodeCommunity = nodeCommunitiesMap.get(nodeCommunity);
		Set<Integer> communities = new HashSet<Integer>();
		for(int nodeFromNodeCommunity : nodesFromNodeCommunity) {
			Set<Integer> neighbors = nodeNeighbours.get(nodeFromNodeCommunity);
			for(int neighbor : neighbors) {
				communities.add(nodeToCommunityMap.get(neighbor));
			}
		}
		return communities;
	}
	
	public void moveNodeCommunity(int nodeCommunity, int toCommunity) throws ExecutionException {
		int fromCommunity = nodeCommunitiesToCommunities.get(nodeCommunity);
		nodeCommunitiesToCommunities.put(nodeCommunity, toCommunity);
		Set<Integer> nodesFromCommunity = communitiesMap.get(fromCommunity);
		communitiesMap.invalidate(fromCommunity);
		communitiesMap.get(toCommunity).addAll(nodesFromCommunity);
		Set<Integer> nodesFromNodeCommunity = nodeCommunitiesMap.get(nodeCommunity);
		for(int nodeFromCommunity : nodesFromNodeCommunity) {
			nodeToCommunityMap.put(nodeFromCommunity, toCommunity);
		}
	}
	
	public double getNodeCommunityWeight(int nodeCommunity) throws ExecutionException {
		Set<Integer> nodes = nodeCommunitiesMap.get(nodeCommunity);
		double weight = 0;
		for(int node : nodes) {
			weight += nodeWeights.get(node);
		}
		return weight;
	}
	
	public int getCommunity(int nodeCommunity) throws ExecutionException {
		return nodeCommunitiesToCommunities.get(nodeCommunity);
	}
	
	public int getCommunitySize(int community) throws ExecutionException {
		return communitySize.get(community);
	}
	
	public double getEdgesInsideCommunity(int nodeCommunity, int community) throws ExecutionException {
		Set<Integer> nodeCommunityNodes = nodeCommunitiesMap.get(nodeCommunity);
		Set<Integer> communityNodes = communitiesMap.get(community);
		double edges = 0;
		for(int nodeCommunityNode : nodeCommunityNodes) {
			for(int communityNode : communityNodes) {
				if(nodeNeighbours.get(nodeCommunityNode).contains(communityNode)) {
					edges++;
				}
			}
		}
		return edges;
	}
	
	public void reInitializeCommunities() {
		nodeCommunitiesMap.invalidateAll();
		communitiesMap.invalidateAll();
		nodeToCommunityMap.invalidateAll();
		communitySize.invalidateAll();
		nodeCommunitiesToCommunities.invalidateAll();
	}
	
}
