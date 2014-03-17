package eu.socialsensor.clustering;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.graphdatabases.Neo4jGraphDatabase;
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
//		GraphDatabase titanDatabase= new TitanGraphDatabase();
//		titanDatabase.open("data/titan");
//
//		LouvainMethodCache lm1 = new LouvainMethodCache(titanDatabase, 50, false); 
//		long start = System.currentTimeMillis();
//		lm1.computeModularity();
//		long time = System.currentTimeMillis() - start;
//		System.out.println(time / 1000.0);
//		lm1.test(titanDatabase);
//		
//		LouvainMethodCache lm2 = new LouvainMethodCache(titanDatabase, 50, false); 
//		start = System.currentTimeMillis();
//		lm2.computeModularity();
//		time = System.currentTimeMillis() - start;
//		System.out.println(time / 1000.0);
//		lm2.test(titanDatabase);
//		
//		GraphDatabase orieDatabase = new OrientGraphDatabase();
//		orieDatabase.open("data/orient");
//		LouvainMethodCache lm2 = new LouvainMethodCache(orieDatabase, 500, false); 
//		lm2.computeModularity();
//		lm2.test(orieDatabase);
		
		GraphDatabase neo4jDatabase = new Neo4jGraphDatabase();
		neo4jDatabase.open("data/neo4j");
		
		LouvainMethodCache lm3 = new LouvainMethodCache(neo4jDatabase, 100, false); 
		long start = System.currentTimeMillis();
		lm3.computeModularity();
		long time = System.currentTimeMillis() - start;
		System.out.println(time / 1000.0);
		lm3.test(neo4jDatabase);
		
		LouvainMethodCache lm4 = new LouvainMethodCache(neo4jDatabase, 100, false); 
		start = System.currentTimeMillis();
		lm4.computeModularity();
		time = System.currentTimeMillis() - start;
		System.out.println(time / 1000.0);
		lm4.test(neo4jDatabase);
		
	}
	
	
	
	public void test(GraphDatabase graphDatabase) {
		
				
		Utils utils = new Utils();
		Map<Integer, List<Integer>> actualCommunities = utils.mapNodesToCommunities("data/community.dat");
		Map<Integer, List<Integer>> communities = graphDatabase.mapCommunities(this.N);

	    Metrics metrics = new Metrics();
	    double nmi = metrics.normalizedMutualInformation(10000, communities, actualCommunities);
	    System.out.println(nmi);
	  
	}
	
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
//		graphDatabase.testCommunities();
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
				zoomOut();
//				System.out.println("=====");
//				graphDatabase.printCommunities();
//				System.out.println("=====");
//				graphDatabase.testCommunities();
//				System.out.println();
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
		this.N = this.graphDatabase.reInitializeCommunities2();
		this.cache.reInitializeCommunities();
		this.communityWeights = new ArrayList<Double>(this.N);
		for(int i = 0; i < this.N; i++) {
			this.communityWeights.add(graphDatabase.getCommunityWeight(i));
		}
	}

}