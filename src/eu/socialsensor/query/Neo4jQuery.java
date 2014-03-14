package eu.socialsensor.query;

import java.util.Iterator;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.Traversal;
import org.neo4j.tooling.GlobalGraphOperations;


public class Neo4jQuery implements Query {
	
	private GraphDatabaseService neo4jGraph = null;
	
	private static enum RelTypes implements RelationshipType {
	    SIMILAR
	}
	
	public static void main(String args[]) {
//		Neo4jQuery test = new Neo4jQuery();
//		test.openDB("data/neo4j");
//		test.findNeighborsOfAllNodes();
//		test.findNodesOfAllEdges();
//		test.findShortestPaths();
//		test.shutdown();
	}
	
//	public void openDB(String neo4jDBDir) {
//		System.out.println("The Neo4j database is now starting . . . .");
//		neo4jGraph = new GraphDatabaseFactory().newEmbeddedDatabase(neo4jDBDir);
//	}
//	
//	public void shutdown() {
//		System.out.println("The Neo4j database is now shuting down . . . .");
//		if(neo4jGraph != null) {
//			neo4jGraph.shutdown();
//		}
//	}
	
	public Neo4jQuery(GraphDatabaseService neo4jGraph) {
		this.neo4jGraph = neo4jGraph;
	}
	
	public void findNeighborsOfAllNodes() {
		for(Node n : GlobalGraphOperations.at(neo4jGraph).getAllNodes()) {
			Traverser traverse = Traversal.description()
					.evaluator(Evaluators.fromDepth(1))
					.evaluator(Evaluators.toDepth(1))
					.evaluator(Evaluators.excludeStartPosition())
					.traverse(n);
		}
	}
	
	public void findNodesOfAllEdges() {
		for(Relationship r : GlobalGraphOperations.at(neo4jGraph).getAllRelationships()) {
			Node startNode = r.getStartNode();
			Node endNode = r.getEndNode();
		}
	}	
	
	public void findShortestPaths() {
		PathFinder<Path> finder = GraphAlgoFactory.shortestPath(Traversal.expanderForTypes(RelTypes.SIMILAR),20);
		Iterator<Node> nodes = GlobalGraphOperations.at(neo4jGraph).getAllNodes().iterator();
		nodes.next();
		Node n1 = nodes.next();
		int iterCount = 0;
		while(iterCount < 10) {
			Node n2 = nodes.next();
			Path path = finder.findSinglePath(n1, n2);
			System.out.println(n1.getProperty("nodeId")+" and "+n2.getProperty("nodeId")+" has length: "+path.length());
			iterCount++;
		}
	}

	
}
