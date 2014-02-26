package eu.socialsensor.main;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
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
import org.neo4j.kernel.Traversal;
import org.neo4j.tooling.GlobalGraphOperations;

import com.tinkerpop.blueprints.Vertex;

public class Neo4jGraphDatabase implements GraphDatabase {

	GraphDatabaseService neo4jGraph = null;
	private Index<Node> nodeIndex = null;
	
	public static void main(String args[]) {
		Neo4jGraphDatabase test = new Neo4jGraphDatabase();
		test.open("data/neo4j");
//		System.out.println(test.getNodeCount());
//		System.out.println(test.getNodeIds().size());
		System.out.println(test.getNeighborsIds(1));
		System.out.println(test.getNodeDegree(1));
	}
	
	@Override
	public void open(String dbPath) {
		neo4jGraph = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
		try(Transaction tx = neo4jGraph.beginTx()) {
			nodeIndex = neo4jGraph.index().forNodes("nodes");
			tx.success();
			tx.close();
		}
//		Node n = nodeIndex.get("nodeId", 1).getSingle();
//		System.out.println(n.getProperty("nodeId"));
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

	@Override
	public List<Long> getNodeIds() {
		List<Long> nodes = new ArrayList<Long>();
		try(Transaction tx = neo4jGraph.beginTx()) {
			for(Node n : GlobalGraphOperations.at(neo4jGraph).getAllNodes()) {
				String nodeId = (String)n.getProperty("nodeId");
				nodes.add(Long.valueOf(nodeId));
			}
			tx.success();
			tx.close();
		}
		return nodes;
	}

	@Override
	public List<Long> getNeighborsIds(long nodeId) {
		List<Long> neighbours = new ArrayList<Long>();
		try (Transaction tx = neo4jGraph.beginTx()) {
			Node n = nodeIndex.get("nodeId", nodeId).getSingle();
			Traverser traverse = Traversal.description()
					.evaluator(Evaluators.fromDepth(1))
					.evaluator(Evaluators.toDepth(1))
					.evaluator(Evaluators.excludeStartPosition())
					.traverse(n);
			for(Node neighbour : traverse.nodes()) {
				String neighbourId = (String)neighbour.getProperty("nodeId");
				neighbours.add(Long.valueOf(neighbourId));
			}
			tx.success();
			tx.close();
		}
		
		return neighbours;
	}

	@Override
	public double getNodeDegree(long nodeId) {
		Node n;
		int nodeDegree;
		try (Transaction tx = neo4jGraph.beginTx()) {
			n = nodeIndex.get("nodeId", nodeId).getSingle();
			nodeDegree = n.getDegree();
			tx.success();
			tx.close();
		}
		return (double)nodeDegree;
	}

	

	@Override
	public void shutdown() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void shutdownMassiveGraph() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void createGraphForSingleLoad(String dbPath) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void createGraphForMassiveLoad(String dbPath) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Iterable getNodes() {
		// TODO Auto-generated method stub
		return null;
	}

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
	public LinkedList<Vertex> getNodesFromCommunity(int community) {
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
	
}
