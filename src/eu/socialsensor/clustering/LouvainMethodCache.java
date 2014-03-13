package eu.socialsensor.clustering;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.graphdatabases.OrientGraphDatabase;
import eu.socialsensor.graphdatabases.TitanGraphDatabase;
import eu.socialsensor.utils.Metrics;
import eu.socialsensor.utils.Utils;

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
		GraphDatabase graphDatabase = new TitanGraphDatabase();
		graphDatabase.open("data/titan");
		
//		GraphDatabase graphDatabase = new OrientGraphDatabase();
//		graphDatabase.open("data/orient");
		
//		GraphDatabase graphDatabase = new Neo4jGraphDatabase();
//		graphDatabase.open("data/neo4j");
		
		LouvainMethodCache lm = new LouvainMethodCache(graphDatabase, 1000, false);
//		lm.execute(graphDatabase);
		
		lm.test(graphDatabase);
	}
	
	
	
	public void test(GraphDatabase graphDatabase) {
		
				
		Utils utils = new Utils();
		Map<Integer, List<Integer>> actualCommunities = utils.mapNodesToCommunities("data/community.dat");
		Map<Integer, List<Integer>> communities = graphDatabase.mapCommunities(this.N);

	    Metrics metrics = new Metrics();
	    double nmi = metrics.normalizedMutualInformation(1000, communities, actualCommunities);
	    System.out.println(nmi);
	  
	}
	
	public LouvainMethodCache(GraphDatabase graphDatabase, int cacheSize, boolean isRandomized) throws ExecutionException {
		this.graphDatabase = graphDatabase;
		this.isRandomized = isRandomized;
		CACHE_SIZE = cacheSize;
		initilize();
		cache = new Cache(graphDatabase, cacheSize);
		computeModularity(graphDatabase, resolution, isRandomized);
	}
	
	private void initilize() {
		this.N = graphDatabase.getNodeCount();
		this.graphWeightSum = graphDatabase.getGraphWeightSum() / 2;
		
		this.communityWeights = new ArrayList<Double>(this.N);
		for(int i = 0; i < this.N; i++) {
			this.communityWeights.add(0.0);
		}
		
		graphDatabase.initCommunityProperty();		
	}
	
	
	private void computeModularity(GraphDatabase graphDatabase, double currentResolution, boolean randomized) throws ExecutionException {
		Random rand = new Random();
//		graphDatabase.testCommunities();
		boolean someChange = true;
		while(someChange) {
			someChange = false;
			boolean localChange = true;
			while(localChange) {
				localChange = false;
				int start = 0;
				if(randomized) {
					start = Math.abs(rand.nextInt()) % this.N;
				}
				int step = 0;
				for(int i = start; step < this.N; i = (i + 1) % this.N) {
					step++;
					int bestCommunity = updateBestCommunity(graphDatabase, i, currentResolution);
					if((cache.getCommunity(i) != bestCommunity) && (this.communityUpdate)) {
						
						cache.moveNodeCommunity(i, bestCommunity);
						graphDatabase.moveNode(i, bestCommunity);
						
						double bestCommunityWeight = this.communityWeights.get(bestCommunity);
						
						bestCommunityWeight += cache.getNodeCommunityWeight(i);					
						this.communityWeights.set(bestCommunity, bestCommunityWeight);
						localChange = true;
//						graphDatabase.testCommunities();
//						graphDatabase.printCommunities();
//						System.out.println();
					}
					
					this.communityUpdate = false;
//					System.out.println();
				}
//				graphDatabase.printCommunities();
				someChange = localChange || someChange;
			}
			if(someChange) {
				zoomOut(graphDatabase);
//				System.out.println("=====");
//				graphDatabase.printCommunities();
//				System.out.println("=====");
//				graphDatabase.testCommunities();
//				System.out.println();
			}
		}
	}
	
	private int updateBestCommunity(GraphDatabase graphDatabase, int node, double currentResolution) throws ExecutionException {
		int bestCommunity = 0;
		double best = 0;
		Set<Integer> communities = cache.getCommunitiesConnectedToNodeCommunities(node);
		for(int community : communities) {
			double qValue = q(graphDatabase, node, community, currentResolution);
			if(qValue > best) {
				best = qValue;
				bestCommunity = community;
				this.communityUpdate = true;
			}
		}
		return bestCommunity;
	}

	
	
	
	private double q(GraphDatabase graphDatabase, int nodeCommunity, int community, double currentResolution) throws ExecutionException {
		double edgesInCommunity = cache.getEdgesInsideCommunity(nodeCommunity, community);	
		double communityWeight = this.communityWeights.get(community);
		double nodeWeight = cache.getNodeCommunityWeight(nodeCommunity);
		double qValue = currentResolution * edgesInCommunity - (nodeWeight * communityWeight) / (2.0 * this.graphWeightSum);
		int actualNodeCom = cache.getCommunity(nodeCommunity);		
		int communitySize = cache.getCommunitySize(community);
		
		if((actualNodeCom == community) && (communitySize > 1)) {
			qValue = currentResolution * edgesInCommunity - (nodeWeight * (communityWeight - nodeWeight)) / (2.0 * this.graphWeightSum);
		}
		if ((actualNodeCom == community) && (communitySize == 1)) {
			qValue = 0.;
		}
		return qValue;
	}
	
	public void zoomOut(GraphDatabase graphDatabase) {		
		this.N = graphDatabase.reInitializeCommunities2();
		cache.reInitializeCommunities();
		this.communityWeights = new ArrayList<Double>(this.N);
		for(int i = 0; i < this.N; i++) {
			this.communityWeights.add(graphDatabase.getCommunityWeight(i));
		}
	}

}