package eu.socialsensor.clustering;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;


import eu.socialsensor.main.GraphDatabase;
import eu.socialsensor.main.TitanGraphDatabase;
import eu.socialsensor.utils.Metrics;
import eu.socialsensor.utils.Utils;

public class LouvainMethod {
	
    private boolean isRandomized = false;
    //private boolean useWeight = true;
    private double resolution = 1.;
    
    double graphWeightSum;
    int N;
	
	public static void main(String args[]) {
		GraphDatabase graphDatabase= new TitanGraphDatabase();
		graphDatabase.open("data/titan");
		
//		GraphDatabase graphDatabase = new OrientGraphDatabase();
//		graphDatabase.open("data/orient");
		
//		GraphDatabase graphDatabase = new Neo4jGraphDatabase();
//		graphDatabase.open("data/neo4j");
		
		LouvainMethod lm = new LouvainMethod();
		lm.execute(graphDatabase);
		
//		lm.test(graphDatabase);
	}
	
	
	
	public void test(GraphDatabase graphDatabase) {
		
		System.out.println(graphDatabase.getNodeCount());
		
//		Utils utils = new Utils();
//		HashMap<Integer, List<Integer>> communities = new HashMap<Integer, List<Integer>>();
//
//	    Map<Integer, List<Integer>> actualCommunities = utils.mapNodesToCommunities("data/community.dat");
//	    
//	    Metrics metrics = new Metrics();
//	    double nmi = metrics.normalizedMutualInformation(1000, communities, actualCommunities);
//	    System.out.println(nmi);
	  
	}
	
	public void execute(GraphDatabase graphDatabase) {
		this.N = graphDatabase.getNodeCount();
		this.graphWeightSum = graphDatabase.getGraphWeightSum();
		graphDatabase.initCommunityProperty();
        computeModularity(graphDatabase, resolution, isRandomized);
    }
	
	public void computeModularity(GraphDatabase graphDatabase, double currentResolution, boolean randomized) {
		Random rand = new Random();
		graphDatabase.testCommunities();
		boolean someChange = true;
		while(someChange) {
			someChange = false;
			boolean localChange = true;
			while(localChange) {
				int start = 0;
				if(randomized) {
					start = Math.abs(rand.nextInt()) % this.N;
				}
				int step = 0;
				for(int i = start; step < this.N; i = (i + 1) % this.N) {
					step++;
					int bestCommunity = updateBestCommunity(graphDatabase, i, currentResolution);
					if((i != bestCommunity)) {
						graphDatabase.moveNode(i, bestCommunity);
						localChange = true;
						graphDatabase.testCommunities();
						System.out.println();
					}
				}
				someChange = localChange || someChange;
			}
			if(someChange) {
				this.N = graphDatabase.getNumberOfCommunities();
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
			}
		}
		return bestCommunity;
	}

	
	
	
	private double q(GraphDatabase graphDatabase, int node, int community, double currentResolution) {
		graphDatabase.getNodesFromCommunity(community);
		double edgesInCommunity = graphDatabase.getEdgesInsideCommunity(node, community);
		double communityWeight = graphDatabase.getCommunityWeight(community);
		double nodeWeight = graphDatabase.getNodeCommunityWeight(node);
		System.out.println(nodeWeight);
		double qValue = currentResolution * edgesInCommunity - (nodeWeight * communityWeight) / (2.0 * this.graphWeightSum);
		if((node == community) && (graphDatabase.getNodesFromCommunity(node).size() > 1)) {
			qValue = currentResolution * edgesInCommunity - (nodeWeight * (communityWeight - nodeWeight)) / (2.0 * this.graphWeightSum);
		}
		
		if ((node == community) && graphDatabase.getNodesFromCommunity(node).size() == 1) {
			qValue = 0.;
		}
		return qValue;
	}

}