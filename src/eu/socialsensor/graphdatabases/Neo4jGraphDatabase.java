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
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.kernel.Traversal;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import com.tinkerpop.blueprints.Vertex;

import eu.socialsensor.insert.Insertion;
import eu.socialsensor.insert.Neo4jMassiveInsertion;
import eu.socialsensor.insert.Neo4jSingleInsertion;

public class Neo4jGraphDatabase implements GraphDatabase {

	GraphDatabaseService neo4jGraph = null;
	private Index<Node> nodeIndex = null;
	
	private BatchInserter inserter = null;
	private BatchInserterIndexProvider indexProvider = null;
	private BatchInserterIndex nodes = null;
	
	public static void main(String args[]) {
		Neo4jGraphDatabase test = new Neo4jGraphDatabase();
		test.open("data/neo4j");
//		System.out.println(test.getNodeCount());
//		System.out.println(test.getNodeIds().size());
	}
	
	@Override
	public void open(String dbPath) {
		System.out.println("Opening Neo4j Graph Database . . . .");
		neo4jGraph = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
		try(Transaction tx = neo4jGraph.beginTx()) {
			nodeIndex = neo4jGraph.index().forNodes("nodes");
			
			
			int counter = 0;
			for(Node n : GlobalGraphOperations.at(neo4jGraph).getAllNodes()) {
				counter++;
			}
			System.out.println(counter);
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
		int nodeCount = 0;
		try(Transaction tx = neo4jGraph.beginTx()) {
			for(Node n : GlobalGraphOperations.at(neo4jGraph).getAllNodes()) {
				nodeCount++;
			}
			tx.success();
			tx.close();
		}
		return nodeCount;
	}

//	@Override
//	public List<Long> getNodeIds() {
//		List<Long> nodes = new ArrayList<Long>();
//		try(Transaction tx = neo4jGraph.beginTx()) {
//			for(Node n : GlobalGraphOperations.at(neo4jGraph).getAllNodes()) {
//				String nodeId = (String)n.getProperty("nodeId");
//				nodes.add(Long.valueOf(nodeId));
//			}
//			tx.success();
//			tx.close();
//		}
//		return nodes;
//	}

	@Override
	public Set<Integer> getNeighborsIds(int nodeId) {
		Set<Integer> neighbours = new HashSet<Integer>();
		try (Transaction tx = neo4jGraph.beginTx()) {
			Node n = nodeIndex.get("nodeId", nodeId).getSingle();
			Traverser traverse = Traversal.description()
					.evaluator(Evaluators.fromDepth(1))
					.evaluator(Evaluators.toDepth(1))
					.evaluator(Evaluators.excludeStartPosition())
					.traverse(n);
			for(Node neighbour : traverse.nodes()) {
				String neighbourId = (String)neighbour.getProperty("nodeId");
				neighbours.add(Integer.valueOf(neighbourId));
			}
			tx.success();
			tx.close();
		}
		
		return neighbours;
	}

//	@Override
//	public double getNodeDegree(long nodeId) {
//		Node n;
//		int nodeDegree;
//		try (Transaction tx = neo4jGraph.beginTx()) {
//			n = nodeIndex.get("nodeId", nodeId).getSingle();
//			nodeDegree = n.getDegree();
//			tx.success();
//			tx.close();
//		}
//		return (double)nodeDegree;
//	}

	

	

	



//	@Override
//	public Iterable getNodes() {
//		// TODO Auto-generated method stub
//		return null;
//	}

	@Override
	public void initCommunityProperty() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities) {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public double getNodeInDegree(Vertex vertex) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getNodeOutDegree(Vertex vertex) {
		// TODO Auto-generated method stub
		return 0;
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
	public void testCommunities() {
		// TODO Auto-generated method stub
		
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
	public double getNodeWeight(int nodeId) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getCommunityFromNode(int nodeId) {
		// TODO Auto-generated method stub
		return 0;
	}

	
	
}
