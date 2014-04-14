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
 * MassiveInsertionBenchmark implementation
 * @author sotbeis
 * @email sotbeis@iti.gr
 */
public class MassiveInsertionBenchmark implements Benchmark{
	
	private final String resultFile = "MIWResults.txt";
	private String datasetDir;
	
	private Logger logger = Logger.getLogger(MassiveInsertionBenchmark.class);
	
	public MassiveInsertionBenchmark(String datasetDir) {
		this.datasetDir = datasetDir;
	}
	
	@Override
	public void startBenchmark() {
		logger.setLevel(Level.INFO);
		logger.info("Executing Massive Insertion Benchmark . . . .");
		
		double[] orientTimes = new double[6];
		double[] titanTimes = new double[6];
		double[] neo4jTimes = new double[6];
		
		//Scenario 1
		logger.info("Scenario 1");
		orientTimes[0] = orientMassiveInsertionBenchmark();
		titanTimes[0] = titanMassiveInsertionBenchmark();
		neo4jTimes[0] = neo4jMassiveInsertionBenchmark();
		
		//Scenario 2
		logger.info("Scenario 2");
		orientTimes[1] = orientMassiveInsertionBenchmark();
		neo4jTimes[1] = neo4jMassiveInsertionBenchmark();
		titanTimes[1] = titanMassiveInsertionBenchmark();
		
		//Scenario 3
		logger.info("Scenario 3");
		neo4jTimes[2] = neo4jMassiveInsertionBenchmark();
		orientTimes[2] = orientMassiveInsertionBenchmark();
		titanTimes[2] = titanMassiveInsertionBenchmark();
		
		//Scenario 4
		logger.info("Scenario 4");
		neo4jTimes[3] = neo4jMassiveInsertionBenchmark();
		titanTimes[3] = titanMassiveInsertionBenchmark();
		orientTimes[3] = orientMassiveInsertionBenchmark();
		
		//Scenario 5
		logger.info("Scenario 5");
		titanTimes[4] = titanMassiveInsertionBenchmark();
		neo4jTimes[4] = neo4jMassiveInsertionBenchmark();
		orientTimes[4] = orientMassiveInsertionBenchmark();
		
		//Scenario 6
		logger.info("Scenario 6");
		titanTimes[5] = titanMassiveInsertionBenchmark();
		orientTimes[5] = orientMassiveInsertionBenchmark();
		neo4jTimes[5] = neo4jMassiveInsertionBenchmark();
		
		logger.info("Massive Insertion Benchmark finished");
		
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
			out.write("########################################################");
			out.write("\n");
			out.write("######### Massive Insertion Benchmark Results ##########");
			out.write("\n");
			out.write("########################################################");
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
	
	private double orientMassiveInsertionBenchmark() {
		GraphDatabase orientGraphDatabase = new OrientGraphDatabase();
		orientGraphDatabase.createGraphForMassiveLoad(GraphDatabaseBenchmark.ORIENTDB_PATH);
		long start = System.currentTimeMillis();
		orientGraphDatabase.massiveModeLoading(datasetDir);
		long orientTime = System.currentTimeMillis() - start;
		orientGraphDatabase.shutdownMassiveGraph();
		orientGraphDatabase.delete(GraphDatabaseBenchmark.ORIENTDB_PATH);
		return orientTime/1000.0;
	}
	
	private double titanMassiveInsertionBenchmark() {
		GraphDatabase titanGraphDatabase = new TitanGraphDatabase();
		titanGraphDatabase.createGraphForMassiveLoad(GraphDatabaseBenchmark.TITANDB_PATH);
		long start = System.currentTimeMillis();
		titanGraphDatabase.massiveModeLoading(datasetDir);
		long titanTime = System.currentTimeMillis() - start;
		titanGraphDatabase.shutdownMassiveGraph();
		titanGraphDatabase.delete(GraphDatabaseBenchmark.TITANDB_PATH);
		return titanTime/1000.0;
	}
	
	private double neo4jMassiveInsertionBenchmark() {
		GraphDatabase neo4jGraphDatabase = new Neo4jGraphDatabase();
		neo4jGraphDatabase.createGraphForMassiveLoad(GraphDatabaseBenchmark.NEO4JDB_PATH);
		long start = System.currentTimeMillis();
		neo4jGraphDatabase.massiveModeLoading(datasetDir);
		long neo4jTime = System.currentTimeMillis() - start;
		neo4jGraphDatabase.shutdownMassiveGraph();
		neo4jGraphDatabase.delete(GraphDatabaseBenchmark.NEO4JDB_PATH);
		return neo4jTime/1000.0;
	}
	
}
