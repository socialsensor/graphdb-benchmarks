package eu.socialsensor.clustering;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import eu.socialsensor.graphdatabases.GraphDatabase;

public class LouvainMethodCache {
	
    boolean isRandomized;
    private double resolution = 1.0;
    private double graphWeightSum;
    private int N;
    private List<Double> communityWeights;
    public int counter = 0;
    private boolean communityUpdate = false;
    
    GraphDatabase graphDatabase;
    Cache cache;
    
    public static int CACHE_SIZE = 1000;
        
	public static void main(String args[]) throws ExecutionException {
		
	}
	
//	public void test(GraphDatabase graphDatabase) {
//				
//		Utils utils = new Utils();
//		Map<Integer, List<Integer>> actualCommunities = utils.mapNodesToCommunities("data/community.dat");
//		Map<Integer, List<Integer>> communities = graphDatabase.mapCommunities(this.N);
//
//	    Metrics metrics = new Metrics();
//	    double nmi = metrics.normalizedMutualInformation(10000, communities, actualCommunities);
//	    System.out.println(nmi);
//	  
//	}
	
	public LouvainMethodCache(GraphDatabase graphDatabase, int cacheSize, boolean isRandomized) throws ExecutionException {
		this.graphDatabase = graphDatabase;
		this.isRandomized = isRandomized;
		CACHE_SIZE = cacheSize;
		initilize();
		cache = new Cache(graphDatabase, cacheSize);
	}
	
	private void initilize() {
		this.N = this.graphDatabase.getNodeCount();
		this.graphWeightSum = this.graphDatabase.getGraphWeightSum() / 2;
		
		this.communityWeights = new ArrayList<Double>(this.N);
		for(int i = 0; i < this.N; i++) {
			this.communityWeights.add(0.0);
		}
		
		this.graphDatabase.initCommunityProperty();		
	}
	
	public void computeModularity() throws ExecutionException {
		System.out.println("Computing communities . . . .");
		Random rand = new Random();
		boolean someChange = true;
		while(someChange) {
			someChange = false;
			boolean localChange = true;
			while(localChange) {
				localChange = false;
				int start = 0;
				if(this.isRandomized) {
					start = Math.abs(rand.nextInt()) % this.N;
				}
				int step = 0;
				for(int i = start; step < this.N; i = (i + 1) % this.N) {
					step++;
					int bestCommunity = updateBestCommunity(i);
					if((this.cache.getCommunity(i) != bestCommunity) && (this.communityUpdate)) {
						
						this.cache.moveNodeCommunity(i, bestCommunity);
						this.graphDatabase.moveNode(i, bestCommunity);
						
						double bestCommunityWeight = this.communityWeights.get(bestCommunity);
						
						bestCommunityWeight += cache.getNodeCommunityWeight(i);					
						this.communityWeights.set(bestCommunity, bestCommunityWeight);
						localChange = true;
					}
					
					this.communityUpdate = false;
				}
				someChange = localChange || someChange;
			}
			if(someChange) {
				zoomOut();
			}
		}
	}
	
	private int updateBestCommunity(int node) throws ExecutionException {
		int bestCommunity = 0;
		double best = 0;
		Set<Integer> communities = this.cache.getCommunitiesConnectedToNodeCommunities(node);
		for(int community : communities) {
			double qValue = q(node, community);
			if(qValue > best) {
				best = qValue;
				bestCommunity = community;
				this.communityUpdate = true;
			}
		}
		return bestCommunity;
	}

	private double q(int nodeCommunity, int community) throws ExecutionException {
		double edgesInCommunity = this.cache.getEdgesInsideCommunity(nodeCommunity, community);	
		double communityWeight = this.communityWeights.get(community);
		double nodeWeight = this.cache.getNodeCommunityWeight(nodeCommunity);
		double qValue = this.resolution * edgesInCommunity - (nodeWeight * communityWeight) / (2.0 * this.graphWeightSum);
		int actualNodeCom = this.cache.getCommunity(nodeCommunity);		
		int communitySize = this.cache.getCommunitySize(community);
		
		if((actualNodeCom == community) && (communitySize > 1)) {
			qValue = this.resolution * edgesInCommunity - (nodeWeight * (communityWeight - nodeWeight)) / (2.0 * this.graphWeightSum);
		}
		if ((actualNodeCom == community) && (communitySize == 1)) {
			qValue = 0.;
		}
		return qValue;
	}
	
	public void zoomOut() {		
		this.N = this.graphDatabase.reInitializeCommunities();
		this.cache.reInitializeCommunities();
		this.communityWeights = new ArrayList<Double>(this.N);
		for(int i = 0; i < this.N; i++) {
			this.communityWeights.add(graphDatabase.getCommunityWeight(i));
		}
	}

}