package eu.socialsensor.clustering;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.esotericsoftware.kryo.util.Util;
import com.esotericsoftware.kryo.util.IdentityMap.Entry;
import com.tinkerpop.blueprints.Vertex;

import eu.socialsensor.clustering.Modularity.ModEdge;
import eu.socialsensor.main.GraphDatabase;
import eu.socialsensor.main.TitanGraphDatabase;
import eu.socialsensor.utils.Metrics;
import eu.socialsensor.utils.Utils;

public class Modularity {
	
    private boolean isCanceled;
    private CommunityStructure structure;
    private boolean isRandomized = false;
    private boolean useWeight = true;
    private double resolution = 1.;
	
	public static void main(String args[]) {
		GraphDatabase graphDatabase= new TitanGraphDatabase();
		graphDatabase.open("data/titan");
		
//		GraphDatabase graphDatabase = new OrientGraphDatabase();
//		graphDatabase.open("data/orient");
		
//		GraphDatabase graphDatabase = new Neo4jGraphDatabase();
//		graphDatabase.open("data/neo4j");
		
		Modularity modularity = new Modularity();
		modularity.execute(graphDatabase);
		
		modularity.test();
	}
	
	public void test() {
		HashMap<Integer, Community> invmap = this.structure.invMap;
		for(Map.Entry<Integer, Community> entry : invmap.entrySet()) {
	    	System.out.println("Community "+entry.getKey());
	    	Community com = entry.getValue();
	    	System.out.println(com.nodes);
	    }
		
//		Utils utils = new Utils();
//		HashMap<Integer, List<Integer>> communities = new HashMap<Integer, List<Integer>>();
//		HashMap<Long, Integer> map = this.structure.map;
//	    for(Map.Entry<Integer, Community> entry : invmap.entrySet()) {
//	    	List<Integer> nodesIndeces = entry.getValue().nodes;
//	    	List<Integer> nodes = new ArrayList<Integer>();
//	    	for(Integer nodeIndex : nodesIndeces) {
//	    		long node = utils.getKeyByValue(map, nodeIndex);
//	    		nodes.add((int)node);
//	    	}
//	    	communities.put(entry.getKey(), nodes);
//	    }
//
//	    Map<Integer, List<Integer>> actualCommunities = utils.mapNodesToCommunities("data/community.dat");
//	    
//	    Metrics metrics = new Metrics();
//	    double nmi = metrics.normalizedMutualInformation(32, communities, actualCommunities);
//	    System.out.println(nmi);
//	    
//	    Map<Community, Integer>[] nodeConnectionsCount = this.structure.nodeConnectionsCount;
//	    for(int i = 0; i < nodeConnectionsCount.length; i++) {
//	    	Map<Community, Integer> ncc = nodeConnectionsCount[i];
//	    	for(Map.Entry<Community, Integer> entry : ncc.entrySet()) {
//	    		System.out.println(entry.getValue());
//	    	}
//	    }
//	    
//	    Map<Community, Float>[] nodeConnectionsWeight = this.structure.nodeConnectionsWeight;
//	    for(int i = 0; i < nodeConnectionsWeight.length; i++) {
//	    	Map<Community, Float> ncc = nodeConnectionsWeight[i];
//	    	for(Map.Entry<Community, Float> entry : ncc.entrySet()) {
//	    		System.out.println(entry.getValue());
//	    	}
//	    }
	}
	
	
	class ModEdge {

        int source;
        int target;
        float weight;

        public ModEdge(int s, int t, float w) {
            source = s;
            target = t;
            weight = w;
        }
        
    }
	
	
	class CommunityStructure {

        HashMap<Modularity.Community, Float>[] nodeConnectionsWeight;
        HashMap<Modularity.Community, Integer>[] nodeConnectionsCount;
        HashMap<Long, Integer> map;
        Community[] nodeCommunities;
        GraphDatabase graphDatabase;
        double[] weights;
        double graphWeightSum;
        LinkedList<ModEdge>[] topology;
        LinkedList<Community> communities;
        int N;
        HashMap<Integer, Community> invMap;

        CommunityStructure(GraphDatabase graphDatabase) {
        	
            this.graphDatabase = graphDatabase;
            N = graphDatabase.getNodeCount();
            invMap = new HashMap<Integer, Community>();
            nodeConnectionsWeight = new HashMap[N];
            nodeConnectionsCount = new HashMap[N];
            nodeCommunities = new Community[N];
            map = new HashMap<Long, Integer>();
            topology = new LinkedList[N];
            communities = new LinkedList<Community>();
            int index = 0;
            weights = new double[N];
            for (Long node : graphDatabase.getNodeIds()) {
                map.put(node, index);
                nodeCommunities[index] = new Community(this);
                nodeConnectionsWeight[index] = new HashMap<Community, Float>();
                nodeConnectionsCount[index] = new HashMap<Community, Integer>();
                weights[index] = 0;
                nodeCommunities[index].seed(index);
                Community hidden = new Community(structure);
                hidden.nodes.add(index);
                invMap.put(index, hidden);
                communities.add(nodeCommunities[index]);
                index++;
//                if (isCanceled) {
//                    return;
//                }
            }

            for (Long node : graphDatabase.getNodeIds()) {
//            	System.out.println(node);
//            	System.out.println("=======");
                int node_index = map.get(node);
                topology[node_index] = new LinkedList<ModEdge>();

                for (Long neighbor : graphDatabase.getNeighborsIds(node)) {
                	System.out.println(neighbor);
                	if (node == neighbor) {
                        continue;
                    }
                    int neighbor_index = map.get(neighbor);
                    float weight = 1;
                    
//                    if (useWeight) {
//                        weight = (float) hgraph.getEdge(node, neighbor).getWeight();
//                    }

                    weights[node_index] += weight;
                    Modularity.ModEdge me = new ModEdge(node_index, neighbor_index, weight);
                    topology[node_index].add(me);
                    Community adjCom = nodeCommunities[neighbor_index];
                    nodeConnectionsWeight[node_index].put(adjCom, weight);
                    nodeConnectionsCount[node_index].put(adjCom, 1);
                    nodeCommunities[node_index].connectionsWeight.put(adjCom, weight);
                    nodeCommunities[node_index].connectionsCount.put(adjCom, 1);
                    nodeConnectionsWeight[neighbor_index].put(nodeCommunities[node_index], weight);
                    nodeConnectionsCount[neighbor_index].put(nodeCommunities[node_index], 1);
                    nodeCommunities[neighbor_index].connectionsWeight.put(nodeCommunities[node_index], weight);
                    nodeCommunities[neighbor_index].connectionsCount.put(nodeCommunities[node_index], 1);
                    graphWeightSum += weight;
                }

                if (isCanceled) {
                    return;
                }
            }
            graphWeightSum /= 2.0;
        }

        private void addNodeTo(int node, Community to) {
            to.add(new Integer(node));
            nodeCommunities[node] = to;

            for (ModEdge e : topology[node]) {
                int neighbor = e.target;

                ////////
                //Remove Node Connection to this community
                Float neighEdgesTo = nodeConnectionsWeight[neighbor].get(to);
                if (neighEdgesTo == null) {
                    nodeConnectionsWeight[neighbor].put(to, e.weight);
                } else {
                    nodeConnectionsWeight[neighbor].put(to, neighEdgesTo + e.weight);
                }
                Integer neighCountEdgesTo = nodeConnectionsCount[neighbor].get(to);
                if (neighCountEdgesTo == null) {
                    nodeConnectionsCount[neighbor].put(to, 1);
                } else {
                    nodeConnectionsCount[neighbor].put(to, neighCountEdgesTo + 1);
                }

                ///////////////////
                Modularity.Community adjCom = nodeCommunities[neighbor];
                Float wEdgesto = adjCom.connectionsWeight.get(to);
                if (wEdgesto == null) {
                    adjCom.connectionsWeight.put(to, e.weight);
                } else {
                    adjCom.connectionsWeight.put(to, wEdgesto + e.weight);
                }

                Integer cEdgesto = adjCom.connectionsCount.get(to);
                if (cEdgesto == null) {
                    adjCom.connectionsCount.put(to, 1);
                } else {
                    adjCom.connectionsCount.put(to, cEdgesto + 1);
                }

                Float nodeEdgesTo = nodeConnectionsWeight[node].get(adjCom);
                if (nodeEdgesTo == null) {
                    nodeConnectionsWeight[node].put(adjCom, e.weight);
                } else {
                    nodeConnectionsWeight[node].put(adjCom, nodeEdgesTo + e.weight);
                }

                Integer nodeCountEdgesTo = nodeConnectionsCount[node].get(adjCom);
                if (nodeCountEdgesTo == null) {
                    nodeConnectionsCount[node].put(adjCom, 1);
                } else {
                    nodeConnectionsCount[node].put(adjCom, nodeCountEdgesTo + 1);
                }

                if (to != adjCom) {
                    Float comEdgesto = to.connectionsWeight.get(adjCom);
                    if (comEdgesto == null) {
                        to.connectionsWeight.put(adjCom, e.weight);
                    } else {
                        to.connectionsWeight.put(adjCom, comEdgesto + e.weight);
                    }

                    Integer comCountEdgesto = to.connectionsCount.get(adjCom);
                    if (comCountEdgesto == null) {
                        to.connectionsCount.put(adjCom, 1);
                    } else {
                        to.connectionsCount.put(adjCom, comCountEdgesto + 1);
                    }

                }
            }
        }

        private void removeNodeFrom(int node, Community from) {

            Community community = nodeCommunities[node];
            for (ModEdge e : topology[node]) {
                int neighbor = e.target;

                ////////
                //Remove Node Connection to this community
                Float edgesTo = nodeConnectionsWeight[neighbor].get(community);
                Integer countEdgesTo = nodeConnectionsCount[neighbor].get(community);
                if (countEdgesTo - 1 == 0) {
                    nodeConnectionsWeight[neighbor].remove(community);
                    nodeConnectionsCount[neighbor].remove(community);
                } else {
                    nodeConnectionsWeight[neighbor].put(community, edgesTo - e.weight);
                    nodeConnectionsCount[neighbor].put(community, countEdgesTo - 1);
                }

                ///////////////////
                //Remove Adjacency Community's connection to this community
                Modularity.Community adjCom = nodeCommunities[neighbor];
                Float oEdgesto = adjCom.connectionsWeight.get(community);
                Integer oCountEdgesto = adjCom.connectionsCount.get(community);
                    adjCom.connectionsWeight.remove(community);
                    if (oCountEdgesto - 1 == 0) {
                    adjCom.connectionsCount.remove(community);
                } else {
                    adjCom.connectionsWeight.put(community, oEdgesto - e.weight);
                    adjCom.connectionsCount.put(community, oCountEdgesto - 1);
                }

                if (node == neighbor) {
                    continue;
                }

                if (adjCom != community) {
                    Float comEdgesto = community.connectionsWeight.get(adjCom);
                    Integer comCountEdgesto = community.connectionsCount.get(adjCom);
                    if (comCountEdgesto - 1 == 0) {
                        community.connectionsWeight.remove(adjCom);
                        community.connectionsCount.remove(adjCom);
                    } else {
                        community.connectionsWeight.put(adjCom, comEdgesto - e.weight);
                        community.connectionsCount.put(adjCom, comCountEdgesto - 1);
                    }
                }

                Float nodeEgesTo = nodeConnectionsWeight[node].get(adjCom);
                Integer nodeCountEgesTo = nodeConnectionsCount[node].get(adjCom);
                if (nodeCountEgesTo - 1 == 0) {
                    nodeConnectionsWeight[node].remove(adjCom);
                    nodeConnectionsCount[node].remove(adjCom);
                } else {
                    nodeConnectionsWeight[node].put(adjCom, nodeEgesTo - e.weight);
                    nodeConnectionsCount[node].put(adjCom, nodeCountEgesTo - 1);
                }

            }
            from.remove(new Integer(node));
        }

        private void moveNodeTo(int node, Community to) {
            Community from = nodeCommunities[node];
            removeNodeFrom(node, from);
            addNodeTo(node, to);
        }

        private void zoomOut() {
            int M = communities.size();
            LinkedList<ModEdge>[] newTopology = new LinkedList[M];
            int index = 0;
            nodeCommunities = new Community[M];
            nodeConnectionsWeight = new HashMap[M];
            nodeConnectionsCount = new HashMap[M];
            HashMap<Integer, Community> newInvMap = new HashMap<Integer, Community>();
            for (int i = 0; i < communities.size(); i++) {//Community com : mCommunities) {
                Community com = communities.get(i);
                 nodeConnectionsWeight[index] = new HashMap<Community, Float>();
                nodeConnectionsCount[index] = new HashMap<Community, Integer>();
                newTopology[index] = new LinkedList<ModEdge>();
                nodeCommunities[index] = new Community(com);
                Set<Community> iter = com.connectionsWeight.keySet();
                double weightSum = 0;

                Community hidden = new Community(structure);
                for (Integer nodeInt : com.nodes) {
                    Community oldHidden = invMap.get(nodeInt);
                    hidden.nodes.addAll(oldHidden.nodes);
                }
                newInvMap.put(index, hidden);
                for (Modularity.Community adjCom : iter) {
                    int target = communities.indexOf(adjCom);
                    float weight = com.connectionsWeight.get(adjCom);
                    if (target == index) {
                        weightSum += 2. * weight;
                    } else {
                        weightSum += weight;
                    }
                    ModEdge e = new ModEdge(index, target, weight);
                    newTopology[index].add(e);
                }
                weights[index] = weightSum;
                nodeCommunities[index].seed(index);

                index++;
            }
            communities.clear();

            for (int i = 0; i < M; i++) {
                Community com = nodeCommunities[i];
                communities.add(com);
                for (ModEdge e : newTopology[i]) {
                    nodeConnectionsWeight[i].put(nodeCommunities[e.target], e.weight);
                    nodeConnectionsCount[i].put(nodeCommunities[e.target], 1);
                    com.connectionsWeight.put(nodeCommunities[e.target], e.weight);
                    com.connectionsCount.put(nodeCommunities[e.target], 1);
                }

            }

            N = M;
            topology = newTopology;
            invMap = newInvMap;
        }
    }
	
	
	class Community {

        double weightSum;
        CommunityStructure structure;
        LinkedList<Integer> nodes;
        HashMap<Modularity.Community, Float> connectionsWeight;
        HashMap<Modularity.Community, Integer> connectionsCount;

        public int size() {
            return nodes.size();
        }

        public Community(Modularity.Community com) {
            structure = com.structure;
            connectionsWeight = new HashMap<Modularity.Community, Float>();
            connectionsCount = new HashMap<Modularity.Community, Integer>();
            nodes = new LinkedList<Integer>();
            //mHidden = pCom.mHidden;
        }

        public Community(CommunityStructure structure) {
            this.structure = structure;
            connectionsWeight = new HashMap<Modularity.Community, Float>();
            connectionsCount = new HashMap<Modularity.Community, Integer>();
            nodes = new LinkedList<Integer>();
        }

        public void seed(int node) {
            nodes.add(node);
            weightSum += structure.weights[node];
            System.out.println();
        }

        public boolean add(int node) {
            nodes.addLast(new Integer(node));
            weightSum += structure.weights[node];
            return true;
        }

        public boolean remove(int node) {
            boolean result = nodes.remove(new Integer(node));
            weightSum -= structure.weights[node];
            if (nodes.size() == 0) {
                structure.communities.remove(this);
            }
            return result;
        }
    }

	
	
	public void execute(GraphDatabase graphDatabase) {
//        isCanceled = false;

//        hgraph.readLock();

        structure = new Modularity.CommunityStructure(graphDatabase);
        int[] comStructure = new int[graphDatabase.getNodeCount()];

        HashMap<String, Double> computedModularityMetrics = computeModularity(graphDatabase, structure, comStructure, 
        		resolution, isRandomized, useWeight);
        System.out.println();
    }
	
	
	protected HashMap<String, Double> computeModularity(GraphDatabase graphDatabase, CommunityStructure theStructure, 
			int[] comStructure, double currentResolution, boolean randomized, boolean weighted) {
        isCanceled = false;
        Random rand = new Random();

//        double totalWeight = theStructure.graphWeightSum;
//        double[] nodeDegrees = theStructure.weights.clone();

        HashMap<String, Double> results = new HashMap<String, Double>();

        boolean someChange = true;
        while (someChange) {
            someChange = false;
            boolean localChange = true;
            while (localChange) {
                localChange = false;
                int start = 0;
                if (randomized) {
                    start = Math.abs(rand.nextInt()) % theStructure.N;
                }
                int step = 0;
                for (int i = start; step < theStructure.N; i = (i + 1) % theStructure.N) {
                    step++;
                    Community bestCommunity = updateBestCommunity(theStructure, i, currentResolution);
                    if ((theStructure.nodeCommunities[i] != bestCommunity) && (bestCommunity != null)) {
                        theStructure.moveNodeTo(i, bestCommunity);
                        localChange = true;
                    }
                }
                
                
                Iterator<Community> iter = theStructure.communities.iterator();
                Map<Long, Integer> map = theStructure.map;
                int comCounter = 1;
                while(iter.hasNext()) {
                	Iterator<Integer> nodesIter = iter.next().nodes.iterator();
                	List<Long> nodes = new ArrayList<Long>();
                	while(nodesIter.hasNext()) {
                		long nodeId = 0;
                		int n = nodesIter.next();
                		for(Map.Entry<Long, Integer> entry : map.entrySet()) {
                			long key = entry.getKey();
                			int value = entry.getValue();
                			if(value == n) {
                				nodeId = key;
                				break;
                			}
                		}
                		nodes.add(nodeId);
                	}
                	System.out.println("Community "+comCounter);
                	System.out.println(nodes);
                	comCounter++;
                }
                
                
                someChange = localChange || someChange;
            }

            if (someChange) {
                theStructure.zoomOut();
            }
        }

        return results;
    }
	
	
	Community updateBestCommunity(CommunityStructure theStructure, int i, double currentResolution) {
        double best = 0.;
        Community bestCommunity = null;
        Set<Community> iter = theStructure.nodeConnectionsWeight[i].keySet();
        for (Community com : iter) {
            double qValue = q(i, com, theStructure, currentResolution);
            if (qValue > best) {
                best = qValue;
                bestCommunity = com;
            }
        }
        return bestCommunity;
    }
	

	
	private double q(int node, Community community, CommunityStructure theStructure, double currentResolution) {
		printCommunity(theStructure.nodeCommunities[node]);
		printCommunity(community);
        Float edgesToFloat = theStructure.nodeConnectionsWeight[node].get(community);
        double edgesTo = 0;
        if (edgesToFloat != null) {
            edgesTo = edgesToFloat.doubleValue();
        }
        double weightSum = community.weightSum;
        double nodeWeight = theStructure.weights[node];
        double qValue = currentResolution * edgesTo - (nodeWeight * weightSum) / (2.0 * theStructure.graphWeightSum);
        if ((theStructure.nodeCommunities[node] == community) && (theStructure.nodeCommunities[node].size() > 1)) {
            qValue = currentResolution * edgesTo - (nodeWeight * (weightSum - nodeWeight)) / (2.0 * theStructure.graphWeightSum);
        }
        if ((theStructure.nodeCommunities[node] == community) && (theStructure.nodeCommunities[node].size() == 1)) {
            qValue = 0.;
        }
        return qValue;
    }

	public void printCommunity(Community community) {
		Map<Long, Integer> map = this.structure.map;
		Iterator<Integer> nodesIter = community.nodes.iterator();
		List<Long> nodes = new ArrayList<Long>();
    	while(nodesIter.hasNext()) {
    		long nodeId = 0;
    		int n = nodesIter.next();
    		for(Map.Entry<Long, Integer> entry : map.entrySet()) {
    			long key = entry.getKey();
    			int value = entry.getValue();
    			if(value == n) {
    				nodeId = key;
    				break;
    			}
    		}
    		nodes.add(nodeId);
    	}
    	System.out.println("===========================");
    	System.out.println(nodes);
	}
	
	
	
}