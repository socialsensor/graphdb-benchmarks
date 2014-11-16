package eu.socialsensor.insert;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;

import eu.socialsensor.benchmarks.SingleInsertionBenchmark;
import eu.socialsensor.graphdatabases.Neo4jGraphDatabase;
import eu.socialsensor.utils.Utils;

/**
 * Implementation of single Insertion in Neo4j
 * graph database
 * 
 * @author sotbeis
 * @email sotbeis@iti.gr
 * 
 */
public class Neo4jSingleInsertion implements Insertion {

	public static String INSERTION_TIMES_OUTPUT_PATH = null;
	
	private static int count;
	
	private GraphDatabaseService neo4jGraph = null;
	ExecutionEngine engine;
	
	private Logger logger = Logger.getLogger(Neo4jSingleInsertion.class);
	
	public Neo4jSingleInsertion(GraphDatabaseService neo4jGraph) {
		this.neo4jGraph = neo4jGraph;
		engine = new ExecutionEngine(this.neo4jGraph);
	}
	
	@Override
	public void createGraph(String datasetDir) {
		logger.setLevel(Level.INFO);
		INSERTION_TIMES_OUTPUT_PATH = SingleInsertionBenchmark.INSERTION_TIMES_OUTPUT_PATH + ".neo4j";
		count++;
		logger.info("Incrementally loading data in Neo4j database . . . .");
		List<Double> insertionTimes = new ArrayList<Double>();
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(datasetDir)));
			String line;
			int lineCounter = 1;
			long start = System.currentTimeMillis();
			long duration;
			Node srcNode, dstNode;
			while((line = reader.readLine()) != null) {
				if(lineCounter > 4) {
					String[] parts = line.split("\t");
					
					srcNode = getOrCreate(parts[0]);
					dstNode = getOrCreate(parts[1]);
					
					Transaction tx = null;
					try {
						tx = ((GraphDatabaseAPI)neo4jGraph).tx().unforced().begin();
						srcNode.createRelationshipTo(dstNode, Neo4jGraphDatabase.RelTypes.SIMILAR);
						tx.success();
						
					}
					catch(Exception e) {
						
					}
					finally {
						if(tx != null) {
							tx.close();
						}
					}
					
					if(lineCounter % 1000 == 0) {
						duration = System.currentTimeMillis() - start;
						insertionTimes.add((double) duration);
						start = System.currentTimeMillis();
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
	
	private Node getOrCreate(String nodeId) {
		Node result = null;
		
		Transaction tx = null;
		try {
			
			tx = ((GraphDatabaseAPI)neo4jGraph).tx().unforced().begin();
			
			String queryString = "MERGE (n:Node {nodeId: {nodeId}}) RETURN n";
		    Map<String, Object> parameters = new HashMap<String, Object>();
		    parameters.put( "nodeId", nodeId);
		    ResourceIterator<Node> resultIterator = engine.execute( queryString, parameters ).columnAs( "n" );
		    result = resultIterator.next();
		    tx.success();
		    
		}
		catch(Exception e) {
			
		}
		finally {
			tx.close();
		}
		
		return result;
	}
	
}