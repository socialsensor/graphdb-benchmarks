package eu.socialsensor.benchmarks;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.graphdatabases.Neo4jGraphDatabase;
import eu.socialsensor.graphdatabases.OrientGraphDatabase;
import eu.socialsensor.graphdatabases.TitanGraphDatabase;
import eu.socialsensor.main.GraphDatabaseBenchmark;
import eu.socialsensor.utils.Utils;

/**
 * FindNodesOfAllEdgesBenchmark implementation
 * @author sotbeis
 * @email sotbeis@iti.gr
 */
public class FindNodesOfAllEdgesBenchmark implements Benchmark {
	
	private final String resultFile = "QW-FAResults.txt";
	
	private Logger logger = Logger.getLogger(FindNodesOfAllEdgesBenchmark.class);
	
	@Override
	public void startBenchmark() {
		logger.setLevel(Level.INFO);
		logger.info("Find Adjacent Nodes of All Edges Benchmark. . . .");
		
		double[] orientTimes = new double[6];
		double[] titanTimes = new double[6];
		double[] neo4jTimes = new double[6];
		
		logger.info("Scenario 1");
		orientTimes[0] = orientFindNodesOfAllEdgesBenchmark();
		titanTimes[0] = titanFindNodesOfAllEdgesBenchmark();
		neo4jTimes[0] = neo4jFindNodesOfAllEdgesBenchmark();
		
		logger.info("Scenario 2");
		orientTimes[1] = orientFindNodesOfAllEdgesBenchmark();
		neo4jTimes[1] = neo4jFindNodesOfAllEdgesBenchmark();
		titanTimes[1] = titanFindNodesOfAllEdgesBenchmark();
		
		logger.info("Scenario 3");
		neo4jTimes[2] = neo4jFindNodesOfAllEdgesBenchmark();
		orientTimes[2] = orientFindNodesOfAllEdgesBenchmark();
		titanTimes[2] = titanFindNodesOfAllEdgesBenchmark();
		
		logger.info("Scenario 4");
		neo4jTimes[3] = neo4jFindNodesOfAllEdgesBenchmark();
		titanTimes[3] = titanFindNodesOfAllEdgesBenchmark();
		orientTimes[3] = orientFindNodesOfAllEdgesBenchmark();
		
		logger.info("Scenario 5");
		titanTimes[4] = titanFindNodesOfAllEdgesBenchmark();
		neo4jTimes[4] = neo4jFindNodesOfAllEdgesBenchmark();
		orientTimes[4] = orientFindNodesOfAllEdgesBenchmark();
		
		logger.info("Scenario 6");
		titanTimes[5] = titanFindNodesOfAllEdgesBenchmark();
		orientTimes[5] = orientFindNodesOfAllEdgesBenchmark();
		neo4jTimes[5] = neo4jFindNodesOfAllEdgesBenchmark();
		
		logger.info("Find Adjacent Nodes of All Edges Benchmark finished");
		
		Utils utils = new Utils();
		
		double meanOrientTime = utils.calculateMean(orientTimes);
		double meanTitanTime = utils.calculateMean(titanTimes);
		double meanNeo4jTime = utils.calculateMean(neo4jTimes);
		double varOrientTime = utils.calculateVariance(meanOrientTime, orientTimes);
		double varTitanTime = utils.calculateVariance(meanTitanTime, titanTimes);
		double varNeo4jTime = utils.calculateVariance(meanNeo4jTime, neo4jTimes);
		double stdOrientTime = utils.calculateStdDeviation(varOrientTime);
		double stdTitanTime = utils.calculateStdDeviation(varTitanTime);
		double stdNeo4jTime = utils.calculateStdDeviation(varNeo4jTime);
		
		String resultsFolder = GraphDatabaseBenchmark.inputPropertiesFile.getProperty("RESULTS_PATH");
		String output = resultsFolder+resultFile;
		logger.info("Write results to "+output);
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(output));
			out.write("##############################################################");
			out.write("\n");
			out.write("##### Find Adjacent Nodes of All Edges Benchmark Results #####");
			out.write("\n");
			out.write("##############################################################");
			out.write("\n");
			out.write("\n");
			out.write("OrientDB execution time");
			out.write("\n");
			out.write("Mean Value: "+meanOrientTime);
			out.write("\n");
			out.write("STD Value: "+stdOrientTime);
			out.write("\n");
			out.write("\n");
			out.write("Titan execution time");
			out.write("\n");
			out.write("Mean Value: "+meanTitanTime);
			out.write("\n");
			out.write("STD Value: "+stdTitanTime);
			out.write("\n");
			out.write("\n");
			out.write("Neo4j execution time");
			out.write("\n");
			out.write("Mean Value: "+meanNeo4jTime);
			out.write("\n");
			out.write("STD Value: "+stdNeo4jTime);
			
			out.flush();
			out.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	private double orientFindNodesOfAllEdgesBenchmark() {
		GraphDatabase orientGraphDatabase = new OrientGraphDatabase();
		orientGraphDatabase.open(GraphDatabaseBenchmark.ORIENTDB_PATH);
		long start = System.currentTimeMillis();
		orientGraphDatabase.nodesOfAllEdgesQuery();
		long orientTime = System.currentTimeMillis() - start;
		orientGraphDatabase.shutdown();
		return orientTime/1000.0;	
	}
	
	private double titanFindNodesOfAllEdgesBenchmark() {
		GraphDatabase titanGraphDatabase = new TitanGraphDatabase();
		titanGraphDatabase.open(GraphDatabaseBenchmark.TITANDB_PATH);
		long start = System.currentTimeMillis();
		titanGraphDatabase.nodesOfAllEdgesQuery();
		long titanTime = System.currentTimeMillis() - start;
		titanGraphDatabase.shutdown();
		return titanTime/1000.0;
	}
	
	private double neo4jFindNodesOfAllEdgesBenchmark() {
		GraphDatabase neo4jGraphDatabase = new Neo4jGraphDatabase();
		neo4jGraphDatabase.open(GraphDatabaseBenchmark.NEO4JDB_PATH);
		long start = System.currentTimeMillis();
		neo4jGraphDatabase.nodesOfAllEdgesQuery();
		long neo4jTime = System.currentTimeMillis() - start;
		neo4jGraphDatabase.shutdown();
		return neo4jTime/1000.0;	
	}

}
