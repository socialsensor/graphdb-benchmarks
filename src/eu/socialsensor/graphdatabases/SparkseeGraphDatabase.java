package eu.socialsensor.graphdatabases;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Iterators;
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
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;

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
	
	private boolean readOnly = false;
	private boolean clusteringWorkload = false;
	
	double totalWeight;
	
	SparkseeConfig sparkseeConfig = new SparkseeConfig();
	Sparksee sparksee =  new Sparksee(sparkseeConfig);
	Database database;
	Session session;
	Graph sparkseeGraph;
	
	int nodeType;
	
	int nodeAttribute;
	int communityAttribute;
	int nodeCommunityAttribute;
	
	int edgeType;
	
	Value value = new Value();
	
	public static void main(String args[]) throws FileNotFoundException {
		Utils utils = new Utils();
		utils.deleteDatabases();
//		utils.createDatabases("./data/enronEdges.txt");
		
//		GraphDatabase sparkseeGraphDatabase = new SparkseeGraphDatabase();
//		sparkseeGraphDatabase.open(GraphDatabaseBenchmark.SPARKSEEDB_PATH);		
		
//		PrintWriter writer = new PrintWriter("/home/sotbeis/Desktop/sparksee.txt");
//		writer.println(sparkseeGraphDatabase.getNodeWeight(15237));
//		writer.close();
		
//		sparkseeGraphDatabase.shutdown();
	}
	
	@Override
	public void open(String dbPath) {
		try {
			this.database = sparksee.open(dbPath + "/SparkseeDB.gdb", readOnly);
			this.session = database.newSession();
			this.sparkseeGraph = session.getGraph();
			
			this.nodeType = sparkseeGraph.findType("node");
			this.nodeAttribute = sparkseeGraph.findAttribute(nodeType, "nodeId");
			this.edgeType = sparkseeGraph.findType("similar");
			
			if(clusteringWorkload) {
				this.communityAttribute = sparkseeGraph.newAttribute(nodeType, "community", DataType.Integer, AttributeKind.Indexed);
				this.nodeCommunityAttribute = sparkseeGraph.newAttribute(nodeType, "nodeCommunity", DataType.Integer, AttributeKind.Indexed);
			}
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
//		Objects nodes = sparkseeGraph.select(nodeType);
//		ObjectsIterator nodesIter = nodes.iterator();
//		int communityAttribute = sparkseeGraph.findAttribute(nodeType, "community");
//		int nodeCommunityAttribute = sparkseeGraph.findAttribute(nodeType, "nodeCommunity");
//		try {
//			PrintWriter writer = new PrintWriter("/home/sotbeis/Desktop/sparksee.txt");
//			while(nodesIter.hasNext()) {
//				long nodeID = nodesIter.next();
//				Value userId =  sparkseeGraph.getAttribute(nodeID, nodeAttribute);
//				Value community = sparkseeGraph.getAttribute(nodeID, communityAttribute);
//				Value nodeCommunity = sparkseeGraph.getAttribute(nodeID, nodeCommunityAttribute);
//				writer.println(userId.toString() + "\t" + community.toString() + "\t" + nodeCommunity.toString());
//			}
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		nodesIter.close();
//		nodes.close();
		
	}

	@Override
	public void createGraphForSingleLoad(String dbPath) {
		try {
			new File("data/SparkseeDB").mkdir();
			database = sparksee.create(dbPath + "/SparkseeDB.gdb", "SparkseeDB");
			session = database.newSession();
			sparkseeGraph = session.getGraph();
			int node = sparkseeGraph.newNodeType("node");
			sparkseeGraph.newAttribute(node, "nodeId", DataType.String, AttributeKind.Unique);
			sparkseeGraph.newEdgeType("similar", true, false);
		} 
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
	}
	
	@Override
	public void createGraphForMassiveLoad(String dbPath) {
		//maybe some more configuration?
		try {
			new File("data/SparkseeDB").mkdir();
			database = sparksee.create(dbPath + "/SparkseeDB.gdb", "SparkseeDB");
			session = database.newSession();
			sparkseeGraph = session.getGraph();
			int node = sparkseeGraph.newNodeType("node");
			sparkseeGraph.newAttribute(node, "nodeId", DataType.String, AttributeKind.Unique);
			sparkseeGraph.newEdgeType("similar", true, false);
		} 
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
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
		long nodeID = sparkseeGraph.findObject(nodeAttribute, value.setString(String.valueOf(nodeId)));
		Objects neighborsObjects = sparkseeGraph.neighbors(nodeID, edgeType, EdgesDirection.Outgoing);
		ObjectsIterator neighborsIter = neighborsObjects.iterator();
		while(neighborsIter.hasNext()) {
			long neighborID = neighborsIter.next();
			Value neighborNodeID = sparkseeGraph.getAttribute(neighborID, nodeAttribute);
			neighbors.add(Integer.valueOf(neighborNodeID.getString()));
		}
		neighborsIter.close();
		neighborsObjects.close();
		return neighbors;
	}

	@Override
	public double getNodeWeight(int nodeId) {
		long nodeID = sparkseeGraph.findObject(nodeAttribute, value.setString(String.valueOf(nodeId)));
		return getNodeOutDegree(nodeID);
	}
	
	public double getNodeInDegree(long node) {
		long inDegree = sparkseeGraph.degree(node, edgeType, EdgesDirection.Ingoing);
		return (double)inDegree;
	}

	public double getNodeOutDegree(long node) {
		long outDegree = sparkseeGraph.degree(node, edgeType, EdgesDirection.Outgoing);
		return (double)outDegree;
	}

	@Override
	public void initCommunityProperty() {
		int communityCounter = 0;
		//basic or indexed attribute?
		Objects nodes = sparkseeGraph.select(nodeType);
		ObjectsIterator nodesIter = nodes.iterator();
		while(nodesIter.hasNext()) {
			long nodeID = nodesIter.next();
			sparkseeGraph.setAttribute(nodeID, communityAttribute, value.setInteger(communityCounter));
			sparkseeGraph.setAttribute(nodeID, nodeCommunityAttribute, value.setInteger(communityCounter));
			communityCounter++;
		}
		nodesIter.close();
		nodes.close();
	}

	@Override
	public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities) {
		Set<Integer> communities = new HashSet<Integer>();
		Objects nodes = sparkseeGraph.select(nodeCommunityAttribute, Condition.Equal, value.setInteger(nodeCommunities));
		ObjectsIterator nodesIter = nodes.iterator();
		while(nodesIter.hasNext()) {
			long nodeID = nodesIter.next();
			Objects neighbors = sparkseeGraph.neighbors(nodeID, edgeType, EdgesDirection.Outgoing);
			ObjectsIterator neighborsIter = neighbors.iterator();
			while(neighborsIter.hasNext()) {
				long neighborID = neighborsIter.next();
				Value community = sparkseeGraph.getAttribute(neighborID, communityAttribute);
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
		Objects nodes = sparkseeGraph.select(communityAttribute, Condition.Equal, value.setInteger(community));
		ObjectsIterator nodesIter = nodes.iterator();
		while(nodesIter.hasNext()) {
			Value nodeId = sparkseeGraph.getAttribute(nodesIter.next(), nodeAttribute);
			nodesFromCommunity.add(Integer.valueOf(nodeId.getString()));
		}
		nodesIter.close();
		nodes.close();
		return nodesFromCommunity;
	}

	@Override
	public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity) {
		Set<Integer> nodesFromNodeCommunity = new HashSet<Integer>();
		Objects nodes = sparkseeGraph.select(nodeCommunityAttribute, Condition.Equal, value.setInteger(nodeCommunity));
		ObjectsIterator nodesIter = nodes.iterator();
		while(nodesIter.hasNext()) {
			Value nodeId = sparkseeGraph.getAttribute(nodesIter.next(), nodeAttribute);
			nodesFromNodeCommunity.add(Integer.valueOf(nodeId.getString()));
		}
		nodesIter.close();
		nodes.close();
		return nodesFromNodeCommunity;
	}

	@Override
	public double getEdgesInsideCommunity(int nodeCommunity, int communityNode) {
		double edges = 0;
		Objects nodesFromNodeCommunitiy = sparkseeGraph.select(nodeCommunityAttribute, Condition.Equal, value.setInteger(nodeCommunity));
		Objects nodesFromCommunity = sparkseeGraph.select(communityAttribute, Condition.Equal, value.setInteger(communityNode));
		ObjectsIterator nodesFromNodeCommunityIter = nodesFromNodeCommunitiy.iterator();
		while(nodesFromNodeCommunityIter.hasNext()) {
			long nodeID = nodesFromNodeCommunityIter.next();
			Objects neighbors = sparkseeGraph.neighbors(nodeID, edgeType, EdgesDirection.Outgoing);
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
		Objects nodesFromCommunity = sparkseeGraph.select(communityAttribute, Condition.Equal, value.setInteger(community));
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
		Objects nodesFromNodeCommunity = sparkseeGraph.select(nodeCommunityAttribute, Condition.Equal, value.setInteger(nodeCommunity));
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
		Objects fromNodes = sparkseeGraph.select(nodeCommunityAttribute, Condition.Equal, value.setInteger(nodeCommunity));
		ObjectsIterator fromNodesIter = fromNodes.iterator();
		while(fromNodesIter.hasNext()) {
			sparkseeGraph.setAttribute(fromNodesIter.next(), communityAttribute, value.setInteger(toCommunity));
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
		Objects nodes = sparkseeGraph.select(nodeType);
		ObjectsIterator nodesIter = nodes.iterator();
		while(nodesIter.hasNext()) {
			long nodeID = nodesIter.next();
			Value communityId = sparkseeGraph.getAttribute(nodeID, communityAttribute);
			if(!initCommunities.containsKey(communityId.getInteger())) {
				initCommunities.put(communityId.getInteger(), communityCounter);
				communityCounter++;
			}
			int newCommunityId = initCommunities.get(communityId.getInteger());
			sparkseeGraph.setAttribute(nodeID, communityAttribute, value.setInteger(newCommunityId));
			sparkseeGraph.setAttribute(nodeID, nodeCommunityAttribute, value.setInteger(newCommunityId));
		}
		nodesIter.close();
		nodes.close();
		return communityCounter;
	}
	
	@Override
	public int getCommunity(int nodeCommunity) {
		long nodeID = sparkseeGraph.findObject(nodeCommunityAttribute, value.setInteger(nodeCommunity));
		Value communityId = sparkseeGraph.getAttribute(nodeID, communityAttribute);
		return communityId.getInteger();
	}
	
	@Override
	public int getCommunityFromNode(int nodeId) {
		long nodeID = sparkseeGraph.findObject(nodeAttribute, value.setString(String.valueOf(nodeId)));
		Value communityId = sparkseeGraph.getAttribute(nodeID, communityAttribute);
		return communityId.getInteger();
	}

	@Override
	public int getCommunitySize(int community) {
		Objects nodesFromCommunities = sparkseeGraph.select(communityAttribute, Condition.Equal, value.setInteger(community));
		ObjectsIterator nodesFromCommunitiesIter = nodesFromCommunities.iterator();
		Set<Integer> nodeCommunities = new HashSet<Integer>();
		while(nodesFromCommunitiesIter.hasNext()) {
			Value nodeCommunityId = sparkseeGraph.getAttribute(nodesFromCommunitiesIter.next(), nodeCommunityAttribute);
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
			Objects nodesFromCommunity = sparkseeGraph.select(communityAttribute, Condition.Equal, value.setInteger(i));
			ObjectsIterator nodesFromCommunityIter = nodesFromCommunity.iterator();
			List<Integer> nodes = new ArrayList<Integer>();
			while(nodesFromCommunityIter.hasNext()) {
				Value nodeId = sparkseeGraph.getAttribute(nodesFromCommunityIter.next(), nodeAttribute);
				nodes.add(Integer.valueOf(nodeId.getString()));
			}
			communities.put(i, nodes);
			nodesFromCommunityIter.close();
			nodesFromCommunity.close();
		}
		return communities;
	}

	@Override
	public void setClusteringWorkload(boolean isClusteringWorkload) {
		this.clusteringWorkload = isClusteringWorkload;
	}
}
