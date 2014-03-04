package eu.socialsensor.insert;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;
//import org.neo4j.kernel.GraphDatabaseAPI;

import org.neo4j.kernel.GraphDatabaseAPI;

import eu.socialsensor.utils.Utils;

public class Neo4jSingleInsertion implements Insertion {

	public static String INSERTION_TIMES_OUTPUT_PATH = "data/neo4j.insertion.times";
	
	private static int count;
	
	private GraphDatabaseService neo4jGraph = null;
	private Index<Node> nodeIndex;
	
	private static enum RelTypes implements RelationshipType {
	    SIMILAR
	}
	
	
	public Neo4jSingleInsertion(GraphDatabaseService neo4jGraph, Index<Node> nodeIndex) {
		this.neo4jGraph = neo4jGraph;
		this.nodeIndex = nodeIndex;
	}
	
	public void createGraph(String datasetDir) {
		count++;
		System.out.println("Loading data in single mode in Neo4j database . . . .");
		List<Double> insertionTimes = new ArrayList<Double>();
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(datasetDir)));
			String line;
			int nodesCounter = 0;
			int lineCounter = 1;
			//Transaction tx = null;
			long start = System.currentTimeMillis();
			long duration;
			Node srcNode, dstNode;
			int blocksCounter = 0;
			while((line = reader.readLine()) != null) {
				if(lineCounter > 4) {
					String[] parts = line.split("\t");
//					System.out.println(parts[0]);
					
					try (Transaction tx = ((GraphDatabaseAPI)neo4jGraph).tx().unforced().begin()) {
//					try (Transaction tx = neo4jGraph.beginTx()) {
						srcNode = nodeIndex.get("nodeId", parts[0]).getSingle();
						if(srcNode == null) {
							srcNode = neo4jGraph.createNode();
							srcNode.setProperty("nodeId", parts[0]);
							nodeIndex.add(srcNode, "nodeId", parts[0]);
							tx.success();
							tx.close();
							nodesCounter++;
						}
					}
					
					
					if(nodesCounter == 1000) {
						duration = System.currentTimeMillis() - start;
						insertionTimes.add((double) duration);
						nodesCounter = 0;
						start = System.currentTimeMillis();
						blocksCounter++;
						System.out.println(blocksCounter);
					}
					
					try (Transaction tx = ((GraphDatabaseAPI)neo4jGraph).tx().unforced().begin()) {
//						try (Transaction tx = neo4jGraph.beginTx()) {
						dstNode = nodeIndex.get("nodeId", parts[1]).getSingle();
						if(dstNode == null) {
							dstNode = neo4jGraph.createNode();
							dstNode.setProperty("nodeId", parts[1]);
							nodeIndex.add(dstNode, "nodeId", parts[1]);
							tx.success();
							tx.close();
							nodesCounter++;
						}
					}
					
					
					try (Transaction tx = ((GraphDatabaseAPI)neo4jGraph).tx().unforced().begin()) {
//					try (Transaction tx = neo4jGraph.beginTx()) {
						srcNode.createRelationshipTo(dstNode, RelTypes.SIMILAR);
						tx.success();
						tx.close();
					}
					
//					tx.finish();
					
					if(nodesCounter == 1000) {
						duration = System.currentTimeMillis() - start;
						insertionTimes.add((double) duration);
						nodesCounter = 0;
						start = System.currentTimeMillis();
						blocksCounter++;
						System.out.println(blocksCounter);
					}
				}
				lineCounter++;
			}
			
			duration = System.currentTimeMillis() - start;
			insertionTimes.add((double) duration);
			reader.close();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		Utils utils = new Utils();
		utils.writeTimes(insertionTimes, Neo4jSingleInsertion.INSERTION_TIMES_OUTPUT_PATH+"."+count);
	}
	
}
