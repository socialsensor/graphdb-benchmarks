package eu.socialsensor.graphdatabases;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.Traversal;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import com.tinkerpop.blueprints.Vertex;

import eu.socialsensor.clustering.LouvainMethodCache;
import eu.socialsensor.insert.Insertion;
import eu.socialsensor.insert.Neo4jMassiveInsertion;
import eu.socialsensor.insert.Neo4jSingleInsertion;

public class Neo4jGraphDatabase implements GraphDatabase {

	GraphDatabaseService neo4jGraph = null;
	private Index<Node> nodeIndex = null;
	
	private BatchInserter inserter = null;
	private BatchInserterIndexProvider indexProvider = null;
	private BatchInserterIndex nodes = null;
	
	public static enum RelTypes implements RelationshipType {
	    SIMILAR
	}
	
	public static Label NODE_LABEL = DynamicLabel.label("node");
	
	public static void main(String args[]) {
		Neo4jGraphDatabase test = new Neo4jGraphDatabase();
		test.open("data/neo4jYoutube");
//		test.initCommunityProperty();
		System.out.println(test.getCommunitiesConnectedToNodeCommunities(1));
//		test.test();
//		test.testCommunities();
		test.shutdown();
	}
	
	@Override
	public void testCommunities() {
		int counter = 0;
		Transaction tx = neo4jGraph.beginTx();
		for(Node n : GlobalGraphOperations.at(neo4jGraph).getAllNodes()) {
			System.out.println("Node: "+n.getProperty("nodeId")+
					" ==> nodeCommunity: "+n.getProperty("nodeCommunity")+
					" ==> community: "+n.getProperty("community"));
			counter++;
			if(counter == 50) {
				break;
			}
		}
		tx.success();
		tx.close();
	}
	public void test() {
		Transaction tx = neo4jGraph.beginTx();
		for(Node n : GlobalGraphOperations.at(neo4jGraph).getAllNodes()) {
			System.out.println("Node: "+n.getProperty("nodeId")+
					" ==> nodeCommunity: "+n.getProperty("nodeCommunity")+
					" ==> community: "+n.getProperty("community"));
		}
//		Label label = DynamicLabel.label("node");
//		Node n = nodeIndex.get("nodeId", 1).getSingle();
//		System.out.println(n.getProperty("community"));
//		for(Label l : n.getLabels()) {
//			System.out.println(l.name());
//		}
//		ResourceIterable<Node> nodes = neo4jGraph.findNodesByLabelAndProperty(label, "community", 1);
//		for(Node n : nodes) {
//			System.out.println(n.getProperty("nodeId"));
//		}
		tx.success();
		tx.close();
	}
	
	@Override
	public void open(String dbPath) {
		System.out.println("Opening Neo4j Graph Database . . . .");
		neo4jGraph = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
		try(Transaction tx = neo4jGraph.beginTx()) {
			nodeIndex = neo4jGraph.index().forNodes("nodes");
			tx.success();
			tx.close();
		}
		
	}

	@Override
	public void createGraphForSingleLoad(String dbPath) {
		System.out.println("Creating Neo4j Graph Database for single load . . . .");
		neo4jGraph = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
		try ( Transaction tx = neo4jGraph.beginTx() ) {
			nodeIndex = neo4jGraph.index().forNodes("nodes");
			tx.success();
			tx.close();
		}
	}

	@Override
	public void createGraphForMassiveLoad(String dbPath) {
		System.out.println("Creating Neo4j Graph Database for massive load . . . .");
		Map<String, String> config = new HashMap<String, String>();
		config.put("cache_type", "none");
		config.put("use_memory_mapped_buffers", "true");
		config.put("neostore.nodestore.db.mapped_memory", "200M");
		config.put("neostore.relationshipstore.db.mapped_memory", "1000M");
		config.put("neostore.propertystore.db.mapped_memory", "250M");
		config.put("neostore.propertystore.db.strings.mapped_memory", "250M");
		inserter = BatchInserters.inserter(dbPath, config);
		indexProvider = new LuceneBatchInserterIndexProvider(inserter);
		nodes = indexProvider.nodeIndex("nodes", MapUtil.stringMap("type", "exact"));
	}
	
	@Override
	public void singleModeLoading(String dataPath) {
		Insertion neo4jSingleInsertion = new Neo4jSingleInsertion(this.neo4jGraph, this.nodeIndex);
		neo4jSingleInsertion.createGraph(dataPath);
	}
	
	@Override
	public void massiveModeLoading(String dataPath) {
		Insertion neo4jMassiveInsertion = new Neo4jMassiveInsertion(this.inserter, this.nodes);
		neo4jMassiveInsertion.createGraph(dataPath);
	}
	
	@Override
	public void shutdown() {
		System.out.println("The Neo4j database is now shuting down . . . .");
		if(neo4jGraph != null) {
			neo4jGraph.shutdown();
			nodeIndex = null;
		}
	}
	
	@Override
	public void shutdownMassiveGraph() {
		System.out.println("Shutting down Neo4j graph for massive load . . . .");
		if(inserter != null) {
			indexProvider.shutdown();
			inserter.shutdown();
			indexProvider = null;
			inserter = null;
		}
	}
	
	@Override
	public int getNodeCount() {
		int nodeCount;
		try (Transaction tx = neo4jGraph.beginTx()) {
			nodeCount = IteratorUtil.count(GlobalGraphOperations.at(neo4jGraph).getAllNodes());
			tx.success();
			tx.close();
		}
		return nodeCount;
	}

	@Override
	public Set<Integer> getNeighborsIds(int nodeId) {
		Set<Integer> neighbours = new HashSet<Integer>();
		try (Transaction tx = neo4jGraph.beginTx()) {
			Node n = nodeIndex.get("nodeId", nodeId).getSingle();
			for(Relationship relationship : n.getRelationships(Direction.OUTGOING, RelTypes.SIMILAR)) {
				Node neighbour = relationship.getOtherNode(n);
				String neighbourId = (String)neighbour.getProperty("nodeId");
				neighbours.add(Integer.valueOf(neighbourId));
			}
			tx.success();
			tx.close();
		}
		return neighbours;
	}
	
	@Override
	public double getNodeWeight(int nodeId) {
		double weight;
		try(Transaction tx = neo4jGraph.beginTx()) {
			Node n = nodeIndex.get("nodeId", nodeId).getSingle();
			weight =  getNodeOutDegree(n);
			tx.success();
			tx.close();
		}
		return weight;
	}
	
	public double getNodeInDegree(Node node) {
		return node.getDegree(Direction.INCOMING);
	}


	public double getNodeOutDegree(Node node) {
		return node.getDegree(Direction.OUTGOING);
	}

	@Override
	public void initCommunityProperty() {
		int communityCounter = 0;
		int transactionCounter = 0;
		Transaction tx = ((GraphDatabaseAPI)neo4jGraph).tx().unforced().begin();
			for(Node n : GlobalGraphOperations.at(neo4jGraph).getAllNodes()) {
				n.setProperty("nodeCommunity", communityCounter);
				n.setProperty("community", communityCounter);
				communityCounter++;
				transactionCounter++;
				if(transactionCounter == LouvainMethodCache.CACHE_SIZE) {
					transactionCounter = 0;
					tx.success();
					tx.close();
					tx = ((GraphDatabaseAPI)neo4jGraph).tx().unforced().begin();
				}
			}
			tx.success();
			tx.close();
	}

	@Override
	public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities) {
		Set<Integer> communities = new HashSet<Integer>();
		try(Transaction tx = neo4jGraph.beginTx()) {
			ResourceIterable<Node> nodes = neo4jGraph.findNodesByLabelAndProperty(Neo4jGraphDatabase.NODE_LABEL, "nodeCommunity", nodeCommunities);
			for(Node n : nodes) {
				for(Relationship r : n.getRelationships(Direction.OUTGOING, RelTypes.SIMILAR)) {
					Node neighbour = r.getOtherNode(n);
					int community = (int)(neighbour.getProperty("community"));
					communities.add(community);
				}
			}
			tx.success();
			tx.close();
		}
		return communities;
	}


	

	@Override
	public double getCommunityWeight(int community) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getEdgesInsideCommunity(int nodes, int communityNodes) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void moveNode(int from, int to) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getNumberOfCommunities() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getGraphWeightSum() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getNodeCommunityWeight(int nodeCommunity) {
		// TODO Auto-generated method stub
		return 0;
	}


	@Override
	public void reInitializeCommunities(Set<Integer> communityIds) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void printCommunities() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getCommunity(int nodeCommunity) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getCommunitySize(int community) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Set<Integer> getCommunityIds() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Integer> getNodesFromCommunity(int community) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int reInitializeCommunities2() {
		// TODO Auto-generated method stub
		return 0;
	}

	

	@Override
	public int getCommunityFromNode(int nodeId) {
		// TODO Auto-generated method stub
		return 0;
	}

	
	
}
