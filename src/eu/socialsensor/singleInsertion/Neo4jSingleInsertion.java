package eu.socialsensor.singleInsertion;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;

public class Neo4jSingleInsertion {

	
	private GraphDatabaseService neo4jGraph = null;
	private Index<Node> nodeIndex = null;
	
	private static enum RelTypes implements RelationshipType {
	    SIMILAR
	}
	
	public static void main(String args[]) {
		Neo4jSingleInsertion test = new Neo4jSingleInsertion();
		test.startup("data/neo4j");
		test.createGraph("data/flickrEdges.txt");
		test.shutdown();
	}
	
	public void startup(String neo4jDBDir) {
		System.out.println("The Neo4j database is now starting . . . .");
		neo4jGraph = new GraphDatabaseFactory().newEmbeddedDatabase(neo4jDBDir);
		nodeIndex = neo4jGraph.index().forNodes("nodes");
	}
	
	public void shutdown() {
		System.out.println("The Neo4j database is now shuting down . . . .");
		if(neo4jGraph != null) {
			neo4jGraph.shutdown();
			nodeIndex = null;
		}
	}
	
	public List<Double> createGraph(String datasetDir) {
		System.out.println("Incrementally creating the Neo4j database . . . .");
		List<Double> insertionTimes = new ArrayList<Double>();
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(datasetDir)));
			String line;
			int nodesCounter = 0;
			int lineCounter = 1;
			int blocksCounter = 0;
			int nodes = 0;
			Transaction tx = null;
			long start = System.currentTimeMillis();
			long duration;
			while((line = reader.readLine()) != null) {
				if(lineCounter > 4) {
					String[] parts = line.split("\t");
					
					Node srcNode = nodeIndex.get("nodeId", parts[0]).getSingle();
					if(srcNode == null) {
						tx = neo4jGraph.beginTx();
						srcNode = neo4jGraph.createNode();
						srcNode.setProperty("nodeId", parts[0]);
						nodeIndex.add(srcNode, "nodeId", parts[0]);
						tx.success();
						tx.finish();
						nodesCounter++;
						nodes++;
					}
					
					if(nodesCounter == 1000) {
						duration = System.currentTimeMillis() - start;
						insertionTimes.add((double) duration);
						nodesCounter = 0;
						start = System.currentTimeMillis();
						
						blocksCounter++;
						System.out.println("Nodes: "+nodes);
						System.out.println("Time: "+duration);
						System.out.println("blocks: "+blocksCounter);
					}
					
					Node dstNode = nodeIndex.get("nodeId", parts[1]).getSingle();
					if(dstNode == null) {
						tx = neo4jGraph.beginTx();
						dstNode = neo4jGraph.createNode();
						dstNode.setProperty("nodeId", parts[1]);
						nodeIndex.add(dstNode, "nodeId", parts[1]);
						tx.success();
						tx.finish();
						nodesCounter++;
						nodes++;
					}
					
					tx = neo4jGraph.beginTx();
					Relationship relationship = srcNode.createRelationshipTo(dstNode, RelTypes.SIMILAR);
					tx.success();
					tx.finish();
					
					if(nodesCounter == 1000) {
						duration = System.currentTimeMillis() - start;
						insertionTimes.add((double) duration);
						nodesCounter = 0;
						start = System.currentTimeMillis();
						
						blocksCounter++;
						System.out.println("Nodes: "+nodes);
						System.out.println("Time: "+duration);
						System.out.println("blocks: "+blocksCounter);
					}
				}
				lineCounter++;
			}
			if(nodesCounter == 1000) {
				duration = System.currentTimeMillis() - start;
				insertionTimes.add((double) duration);
				nodesCounter = 0;
				start = System.currentTimeMillis();
				
				blocksCounter++;
				System.out.println("Nodes: "+nodes);
				System.out.println("Time: "+duration);
				System.out.println("blocks: "+blocksCounter);
			}
			
			reader.close();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		return insertionTimes;
	}
	
}
