package eu.socialsensor.clustering;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.graphdatabases.TitanGraphDatabase;
import eu.socialsensor.utils.Metrics;
import eu.socialsensor.utils.Utils;

public class LouvainMethod {
	
    private boolean isRandomized = false;
    //private boolean useWeight = true;
    private double resolution = 1.0;
    
    private double graphWeightSum;
    private int N;
    private List<Double> communityWeights;
//    private Set<Integer> communityIds;
    
    public int counter = 0;;
    
    private boolean communityUpdate = false;
	
	public static void main(String args[]) {
		GraphDatabase graphDatabase = new TitanGraphDatabase();
		graphDatabase.open("data/titan");
		
//		GraphDatabase graphDatabase = new OrientGraphDatabase();
//		graphDatabase.open("data/orient");
		
//		GraphDatabase graphDatabase = new Neo4jGraphDatabase();
//		graphDatabase.open("data/neo4j");
		
		LouvainMethod lm = new LouvainMethod();
		lm.execute(graphDatabase);
		
		lm.test(graphDatabase);
	}
	
	
	
	public void test(GraphDatabase graphDatabase) {
		
				
		Utils utils = new Utils();
		Map<Integer, List<Integer>> communities = graphDatabase.mapCommunities(this.N);
	    Map<Integer, List<Integer>> actualCommunities = utils.mapNodesToCommunities("data/community1.dat");

	    Metrics metrics = new Metrics();
	    double nmi = metrics.normalizedMutualInformation(128, communities, actualCommunities);
	    System.out.println(nmi);
	  
	}
	
	public void execute(GraphDatabase graphDatabase) {
		this.N = graphDatabase.getNodeCount();
		this.graphWeightSum = graphDatabase.getGraphWeightSum() / 2;
		graphDatabase.initCommunityProperty();
		
		this.communityWeights = new ArrayList<Double>(this.N);
		for(int i = 0; i < this.N; i++) {
			this.communityWeights.add(0.0);
		}
		
        computeModularity(graphDatabase, resolution, isRandomized);
    }
	
	public void computeModularity(GraphDatabase graphDatabase, double currentResolution, boolean randomized) {
		Random rand = new Random();
//		graphDatabase.testCommunities();
		boolean someChange = true;
		while(someChange) {
//			System.out.println(counter++);
			someChange = false;
			boolean localChange = true;
//			this.communityIds  = new HashSet<Integer>();
			while(localChange) {
				localChange = false;
				int start = 0;
				if(randomized) {
					start = Math.abs(rand.nextInt()) % this.N;
				}
				int step = 0;
				for(int i = start; step < this.N; i = (i + 1) % this.N) {
//					System.out.println(counter++);
					step++;
					int bestCommunity = updateBestCommunity(graphDatabase, i, currentResolution);
					System.out.println(i);
					graphDatabase.testCommunities();
					if((graphDatabase.getCommunity(i) != bestCommunity) && (this.communityUpdate)) {
						graphDatabase.moveNode(i, bestCommunity);
						double bestCommunityWeight = this.communityWeights.get(bestCommunity);
						bestCommunityWeight += graphDatabase.getNodeCommunityWeight(i);
						this.communityWeights.set(bestCommunity, bestCommunityWeight);
//						communityIds.add(bestCommunity);
						localChange = true;
						graphDatabase.testCommunities();
//						graphDatabase.printCommunities();
//						System.out.println();
					}
					
					this.communityUpdate = false;
//					System.out.println();
				}
				graphDatabase.printCommunities();
				someChange = localChange || someChange;
			}
			if(someChange) {
				zoomOut(graphDatabase);
				System.out.println("=====");
				graphDatabase.printCommunities();
				System.out.println("=====");
				graphDatabase.testCommunities();
				System.out.println();
			}
		}
	}
	
	private int updateBestCommunity(GraphDatabase graphDatabase, int node, double currentResolution) {
		int bestCommunity = 0;
		double best = 0;
		Set<Integer> communities = graphDatabase.getCommunitiesConnectedToNodeCommunities(node);
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

	
	
	
	private double q(GraphDatabase graphDatabase, int nodeCommunity, int community, double currentResolution) {
		double edgesInCommunity = graphDatabase.getEdgesInsideCommunity(nodeCommunity, community);
		double communityWeight = this.communityWeights.get(community);
		//double communityWeight = graphDatabase.getCommunityWeight(community);
		double nodeWeight = graphDatabase.getNodeCommunityWeight(nodeCommunity);
		double qValue = currentResolution * edgesInCommunity - (nodeWeight * communityWeight) / (2.0 * this.graphWeightSum);
		int actualNodeCom = graphDatabase.getCommunity(nodeCommunity);
		if((actualNodeCom == community) && (graphDatabase.getCommunitySize(community) > 1)) {
			qValue = currentResolution * edgesInCommunity - (nodeWeight * (communityWeight - nodeWeight)) / (2.0 * this.graphWeightSum);
		}
		if ((actualNodeCom == community) && graphDatabase.getCommunitySize(community) == 1) {
			qValue = 0.;
		}
		return qValue;
	}
	
	public void zoomOut(GraphDatabase graphDatabase) {
		Set<Integer> communityIds = graphDatabase.getCommunityIds();
		this.N = communityIds.size();
		graphDatabase.reInitializeCommunities(communityIds);
		this.communityWeights = new ArrayList<Double>(this.N);
		for(int i = 0; i < this.N; i++) {
			this.communityWeights.add(graphDatabase.getCommunityWeight(i));
		}
	}

}