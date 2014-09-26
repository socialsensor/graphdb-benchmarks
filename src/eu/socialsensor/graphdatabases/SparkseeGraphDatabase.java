package eu.socialsensor.graphdatabases;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.sparsity.sparksee.gdb.AttributeKind;
import com.sparsity.sparksee.gdb.Condition;
import com.sparsity.sparksee.gdb.DataType;
import com.sparsity.sparksee.gdb.Database;
import com.sparsity.sparksee.gdb.EdgesDirection;
import com.sparsity.sparksee.gdb.Graph;
import com.sparsity.sparksee.gdb.Objects;
import com.sparsity.sparksee.gdb.ObjectsIterator;
import com.sparsity.sparksee.gdb.Session;
import com.sparsity.sparksee.gdb.Sparksee;
import com.sparsity.sparksee.gdb.SparkseeConfig;
import com.sparsity.sparksee.gdb.Value;

import eu.socialsensor.insert.Insertion;
import eu.socialsensor.insert.SparkseeMassiveInsertion;
import eu.socialsensor.insert.SparkseeSingleInsertion;
import eu.socialsensor.main.GraphDatabaseBenchmark;
import eu.socialsensor.query.Query;
import eu.socialsensor.query.SparkseeQuery;
import eu.socialsensor.utils.Utils;

/**
 * Sparksee graph database implementation
 * 
 * @author sotbeis
 * @email sotbeis@iti.gr
 */
public class SparkseeGraphDatabase implements GraphDatabase {
	
	public static final String INSERTION_TIMES_OUTPUT_PATH = "data/sparksee.insertion.times";
	private static final String LICENCE_KEY = "D5WY4-NXXGP-EF1Z0-JER78";
	
	private boolean readOnly = false;

	double totalWeight;
	
	SparkseeConfig sparkseeConfig;
	Sparksee sparksee;
	Database database;
	Session session;
	Graph sparkseeGraph;
	
	public static int NODE_ATTRIBUTE;
	public static int COMMUNITY_ATTRIBUTE;
	public static int NODE_COMMUNITY_ATTRIBUTE;
	
	public static int NODE_TYPE;
	
	public static int EDGE_TYPE;
	
	Value value = new Value();
	
	public static void main(String args[]) {
		SparkseeGraphDatabase sparkseeGraphDatabase = new SparkseeGraphDatabase();
		
//		sparkseeGraphDatabase.createGraphForMassiveLoad(GraphDatabaseBenchmark.SPARKSEEDB_PATH);
//		sparkseeGraphDatabase.massiveModeLoading("datasets/real/livejournalEdges.txt");
//		sparkseeGraphDatabase.shutdownMassiveGraph();
		
		sparkseeGraphDatabase.open(GraphDatabaseBenchmark.SPARKSEEDB_PATH);
		System.out.println(sparkseeGraphDatabase.getNodeCount());
		System.out.println(sparkseeGraphDatabase.getGraphWeightSum());
		sparkseeGraphDatabase.shutdown();
	}
	
	@Override
	public void open(String dbPath) {
		try {
			sparkseeConfig = new SparkseeConfig();
			sparkseeConfig.setLicense(LICENCE_KEY);
			sparksee = new Sparksee(sparkseeConfig);
			this.database = sparksee.open(dbPath + "/SparkseeDB.gdb", readOnly);
			this.session = database.newSession();
			this.sparkseeGraph = session.getGraph();
			createSchema();
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void createGraphForSingleLoad(String dbPath) {
		try {
			new File(GraphDatabaseBenchmark.SPARKSEEDB_PATH).mkdir();
			sparkseeConfig = new SparkseeConfig();
			sparkseeConfig.setLicense(LICENCE_KEY);
			sparksee = new Sparksee(sparkseeConfig);
			database = sparksee.create(dbPath + "/SparkseeDB.gdb", "SparkseeDB");
			session = database.newSession();
			sparkseeGraph = session.getGraph();
			createSchema();
		} 
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
	}
	
	@Override
	public void createGraphForMassiveLoad(String dbPath) {
		//maybe some more configuration?
		try {
			new File(GraphDatabaseBenchmark.SPARKSEEDB_PATH).mkdir();
			sparkseeConfig = new SparkseeConfig();
			sparkseeConfig.setLicense(LICENCE_KEY);
			sparksee = new Sparksee(sparkseeConfig);
			database = sparksee.create(dbPath + "/SparkseeDB.gdb", "SparkseeDB");
			session = database.newSession();
			sparkseeGraph = session.getGraph();
			createSchema();
		} 
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private void createSchema() {
		NODE_TYPE = sparkseeGraph.newNodeType("node");
		NODE_ATTRIBUTE = sparkseeGraph.newAttribute(NODE_TYPE, "nodeId", DataType.String, AttributeKind.Unique);
		EDGE_TYPE = sparkseeGraph.newEdgeType("similar", true, false);
		COMMUNITY_ATTRIBUTE = sparkseeGraph.newAttribute(NODE_TYPE, "community", DataType.Integer, 
				AttributeKind.Indexed);
		NODE_COMMUNITY_ATTRIBUTE = sparkseeGraph.newAttribute(NODE_TYPE, "nodeCommunity", 
				DataType.Integer, AttributeKind.Indexed);
	}
	
	@Override
	public void massiveModeLoading(String dataPath) {
		Insertion sparkseeMassiveInsertion = new SparkseeMassiveInsertion(session);
		sparkseeMassiveInsertion.createGraph(dataPath);
	}

	@Override
	public void singleModeLoading(String dataPath) {
		Insertion sparkseeSingleInsertion = new SparkseeSingleInsertion(this.session);
		sparkseeSingleInsertion.createGraph(dataPath);
	}

	@Override
	public void shutdown() {
		if(session != null) {
			session.close();
			session = null;
			database.close();
			database = null;
			sparksee.close();
			sparksee = null;
		}
		
	}
	
	@Override
	public void shutdownMassiveGraph() {
		shutdown();	
	}

	@Override
	public void delete(String dbPath) {
		try {
			Thread.sleep(6000);
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		Utils utils = new Utils();
		utils.deleteRecursively(new File(dbPath));
	}


	@Override
	public void shorestPathQuery() {
		Query sparkseeQuery = new SparkseeQuery(this.session);
		sparkseeQuery.findShortestPaths();		
	}

	@Override
	public void neighborsOfAllNodesQuery() {
		Query sparkseeQuery = new SparkseeQuery(this.session);
		sparkseeQuery.findNeighborsOfAllNodes();
	}

	@Override
	public void nodesOfAllEdgesQuery() {
		Query sparkseeQuery = new SparkseeQuery(this.session);
		sparkseeQuery.findNodesOfAllEdges();
	}

	@Override
	public int getNodeCount() {
		return (int)sparkseeGraph.countNodes();
	}

	@Override
	public Set<Integer> getNeighborsIds(int nodeId) {
		Set<Integer> neighbors = new HashSet<Integer>();
		long nodeID = sparkseeGraph.findObject(NODE_ATTRIBUTE, value.setString(String.valueOf(nodeId)));
		Objects neighborsObjects = sparkseeGraph.neighbors(nodeID, EDGE_TYPE, EdgesDirection.Outgoing);
		ObjectsIterator neighborsIter = neighborsObjects.iterator();
		while(neighborsIter.hasNext()) {
			long neighborID = neighborsIter.next();
			Value neighborNodeID = sparkseeGraph.getAttribute(neighborID, NODE_ATTRIBUTE);
			neighbors.add(Integer.valueOf(neighborNodeID.getString()));
		}
		neighborsIter.close();
		neighborsObjects.close();
		return neighbors;
	}

	@Override
	public double getNodeWeight(int nodeId) {
		long nodeID = sparkseeGraph.findObject(NODE_ATTRIBUTE, value.setString(String.valueOf(nodeId)));
		return getNodeOutDegree(nodeID);
	}
	
	public double getNodeInDegree(long node) {
		long inDegree = sparkseeGraph.degree(node, EDGE_TYPE, EdgesDirection.Ingoing);
		return (double)inDegree;
	}

	public double getNodeOutDegree(long node) {
		long outDegree = sparkseeGraph.degree(node, EDGE_TYPE, EdgesDirection.Outgoing);
		return (double)outDegree;
	}

	@Override
	public void initCommunityProperty() {
		int communityCounter = 0;
		//basic or indexed attribute?
		Objects nodes = sparkseeGraph.select(NODE_TYPE);
		ObjectsIterator nodesIter = nodes.iterator();
		while(nodesIter.hasNext()) {
			long nodeID = nodesIter.next();
			sparkseeGraph.setAttribute(nodeID, COMMUNITY_ATTRIBUTE, value.setInteger(communityCounter));
			sparkseeGraph.setAttribute(nodeID, NODE_COMMUNITY_ATTRIBUTE, value.setInteger(communityCounter));
			communityCounter++;
		}
		nodesIter.close();
		nodes.close();
	}

	@Override
	public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities) {
		Set<Integer> communities = new HashSet<Integer>();
		Objects nodes = sparkseeGraph.select(NODE_COMMUNITY_ATTRIBUTE, Condition.Equal, value.setInteger(nodeCommunities));
		ObjectsIterator nodesIter = nodes.iterator();
		while(nodesIter.hasNext()) {
			long nodeID = nodesIter.next();
			Objects neighbors = sparkseeGraph.neighbors(nodeID, EDGE_TYPE, EdgesDirection.Outgoing);
			ObjectsIterator neighborsIter = neighbors.iterator();
			while(neighborsIter.hasNext()) {
				long neighborID = neighborsIter.next();
				Value community = sparkseeGraph.getAttribute(neighborID, COMMUNITY_ATTRIBUTE);
				communities.add(community.getInteger());
			}
			neighborsIter.close();
			neighbors.close();
		}
		nodesIter.close();
		nodes.close();
		return communities;
	}

	@Override
	public Set<Integer> getNodesFromCommunity(int community) {
		Set<Integer> nodesFromCommunity = new HashSet<Integer>();
		Objects nodes = sparkseeGraph.select(COMMUNITY_ATTRIBUTE, Condition.Equal, value.setInteger(community));
		ObjectsIterator nodesIter = nodes.iterator();
		while(nodesIter.hasNext()) {
			Value nodeId = sparkseeGraph.getAttribute(nodesIter.next(), NODE_ATTRIBUTE);
			nodesFromCommunity.add(Integer.valueOf(nodeId.getString()));
		}
		nodesIter.close();
		nodes.close();
		return nodesFromCommunity;
	}

	@Override
	public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity) {
		Set<Integer> nodesFromNodeCommunity = new HashSet<Integer>();
		Objects nodes = sparkseeGraph.select(NODE_COMMUNITY_ATTRIBUTE, Condition.Equal, 
				value.setInteger(nodeCommunity));
		ObjectsIterator nodesIter = nodes.iterator();
		while(nodesIter.hasNext()) {
			Value nodeId = sparkseeGraph.getAttribute(nodesIter.next(), NODE_ATTRIBUTE);
			nodesFromNodeCommunity.add(Integer.valueOf(nodeId.getString()));
		}
		nodesIter.close();
		nodes.close();
		return nodesFromNodeCommunity;
	}

	@Override
	public double getEdgesInsideCommunity(int nodeCommunity, int communityNode) {
		double edges = 0;
		Objects nodesFromNodeCommunitiy = sparkseeGraph.select(NODE_COMMUNITY_ATTRIBUTE, Condition.Equal, 
				value.setInteger(nodeCommunity));
		Objects nodesFromCommunity = sparkseeGraph.select(COMMUNITY_ATTRIBUTE, Condition.Equal, 
				value.setInteger(communityNode));
		ObjectsIterator nodesFromNodeCommunityIter = nodesFromNodeCommunitiy.iterator();
		while(nodesFromNodeCommunityIter.hasNext()) {
			long nodeID = nodesFromNodeCommunityIter.next();
			Objects neighbors = sparkseeGraph.neighbors(nodeID, EDGE_TYPE, EdgesDirection.Outgoing);
			ObjectsIterator neighborsIter = neighbors.iterator();
			while(neighborsIter.hasNext()) {
				if(nodesFromCommunity.contains(neighborsIter.next())) {
					edges++;
				}
			}
			neighborsIter.close();
			neighbors.close();
		}
		nodesFromNodeCommunityIter.close();
		nodesFromCommunity.close();
		nodesFromNodeCommunitiy.close();
		return edges;
	}

	@Override
	public double getCommunityWeight(int community) {
		double communityWeight = 0;
		Objects nodesFromCommunity = sparkseeGraph.select(COMMUNITY_ATTRIBUTE, Condition.Equal, value.setInteger(community));
		ObjectsIterator nodesFromCommunityIter = nodesFromCommunity.iterator();
		if(nodesFromCommunity.size() > 1) {
			while(nodesFromCommunityIter.hasNext()) {
				communityWeight += getNodeOutDegree(nodesFromCommunityIter.next());
			}
		}
		nodesFromCommunityIter.close();
		nodesFromCommunity.close();
		return communityWeight;
	}

	@Override
	public double getNodeCommunityWeight(int nodeCommunity) {
		double nodeCommunityWeight = 0;
		Objects nodesFromNodeCommunity = sparkseeGraph.select(NODE_COMMUNITY_ATTRIBUTE, Condition.Equal, 
				value.setInteger(nodeCommunity));
		ObjectsIterator nodesFromNodeCommunityIter = nodesFromNodeCommunity.iterator();
		if(nodesFromNodeCommunity.size() > 1) {
			while(nodesFromNodeCommunityIter.hasNext()) {
				nodeCommunityWeight += getNodeOutDegree(nodesFromNodeCommunityIter.next());
			}
		}
		nodesFromNodeCommunityIter.close();
		nodesFromNodeCommunity.close();
		return nodeCommunityWeight;
	}

	@Override
	public void moveNode(int nodeCommunity, int toCommunity) {
		Objects fromNodes = sparkseeGraph.select(NODE_COMMUNITY_ATTRIBUTE, Condition.Equal, value.setInteger(nodeCommunity));
		ObjectsIterator fromNodesIter = fromNodes.iterator();
		while(fromNodesIter.hasNext()) {
			sparkseeGraph.setAttribute(fromNodesIter.next(), COMMUNITY_ATTRIBUTE, value.setInteger(toCommunity));
		}
		fromNodesIter.close();
		fromNodes.close();
	}

	@Override
	public double getGraphWeightSum() {
		return (double )sparkseeGraph.countEdges();
	}

	@Override
	public int reInitializeCommunities() {
		Map<Integer, Integer> initCommunities = new HashMap<Integer, Integer>();
		int communityCounter = 0;
		Objects nodes = sparkseeGraph.select(NODE_TYPE);
		ObjectsIterator nodesIter = nodes.iterator();
		while(nodesIter.hasNext()) {
			long nodeID = nodesIter.next();
			Value communityId = sparkseeGraph.getAttribute(nodeID, COMMUNITY_ATTRIBUTE);
			if(!initCommunities.containsKey(communityId.getInteger())) {
				initCommunities.put(communityId.getInteger(), communityCounter);
				communityCounter++;
			}
			int newCommunityId = initCommunities.get(communityId.getInteger());
			sparkseeGraph.setAttribute(nodeID, COMMUNITY_ATTRIBUTE, value.setInteger(newCommunityId));
			sparkseeGraph.setAttribute(nodeID, NODE_COMMUNITY_ATTRIBUTE, value.setInteger(newCommunityId));
		}
		nodesIter.close();
		nodes.close();
		return communityCounter;
	}
	
	@Override
	public int getCommunity(int nodeCommunity) {
		long nodeID = sparkseeGraph.findObject(NODE_COMMUNITY_ATTRIBUTE, value.setInteger(nodeCommunity));
		Value communityId = sparkseeGraph.getAttribute(nodeID, COMMUNITY_ATTRIBUTE);
		return communityId.getInteger();
	}
	
	@Override
	public int getCommunityFromNode(int nodeId) {
		long nodeID = sparkseeGraph.findObject(NODE_ATTRIBUTE, value.setString(String.valueOf(nodeId)));
		Value communityId = sparkseeGraph.getAttribute(nodeID, COMMUNITY_ATTRIBUTE);
		return communityId.getInteger();
	}

	@Override
	public int getCommunitySize(int community) {
		Objects nodesFromCommunities = sparkseeGraph.select(COMMUNITY_ATTRIBUTE, Condition.Equal, value.setInteger(community));
		ObjectsIterator nodesFromCommunitiesIter = nodesFromCommunities.iterator();
		Set<Integer> nodeCommunities = new HashSet<Integer>();
		while(nodesFromCommunitiesIter.hasNext()) {
			Value nodeCommunityId = sparkseeGraph.getAttribute(nodesFromCommunitiesIter.next(), NODE_COMMUNITY_ATTRIBUTE);
			nodeCommunities.add(nodeCommunityId.getInteger());
		}
		nodesFromCommunitiesIter.close();
		nodesFromCommunities.close();
		return nodeCommunities.size();
	}

	@Override
	public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities) {
		Map<Integer, List<Integer>> communities = new HashMap<Integer, List<Integer>>();
		for(int i = 0; i < numberOfCommunities; i++) {
			Objects nodesFromCommunity = sparkseeGraph.select(COMMUNITY_ATTRIBUTE, Condition.Equal, value.setInteger(i));
			ObjectsIterator nodesFromCommunityIter = nodesFromCommunity.iterator();
			List<Integer> nodes = new ArrayList<Integer>();
			while(nodesFromCommunityIter.hasNext()) {
				Value nodeId = sparkseeGraph.getAttribute(nodesFromCommunityIter.next(), NODE_ATTRIBUTE);
				nodes.add(Integer.valueOf(nodeId.getString()));
			}
			communities.put(i, nodes);
			nodesFromCommunityIter.close();
			nodesFromCommunity.close();
		}
		return communities;
	}
	
	@Override
	public boolean nodeExists(int nodeId) {
		Objects nodes = sparkseeGraph.select(NODE_ATTRIBUTE, Condition.Equal, value.setInteger(nodeId));
		ObjectsIterator nodesIter = nodes.iterator();
		if(nodesIter.hasNext()) {
			nodesIter.close();
			nodes.close();
			return true;
		}
		nodesIter.close();
		nodes.close();
		return false;
	}

}
